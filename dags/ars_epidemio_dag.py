# dags/ars_epidemio_dag.py
"""
DAG de surveillance épidémiologique — ARS Occitanie
Pipeline complet : collecte IAS® -> calcul indicateurs -> alertes -> rapport
"""

import json
import logging
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_semaine(context):
    """Retourne la semaine ISO au format YYYY-SXX depuis le contexte Airflow."""
    ed = context["execution_date"]
    return "{}-S{:02d}".format(ed.year, ed.isocalendar()[1])


def _get_chemins(semaine):
    """Retourne (annee, num_sem, archive_base_path)."""
    annee, num_sem = semaine.split("-")
    base = Variable.get("archive_base_path", default_var="/data/ars")
    return annee, num_sem, base


# ---------------------------------------------------------------------------
# Paramètres du DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 6, 24),
    catchup=False,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "occitanie"],
) as dag:

    # -----------------------------------------------------------------------
    # Étape 3 — Init base de données
    # -----------------------------------------------------------------------
    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    # -----------------------------------------------------------------------
    # Étape 4 — Collecte IAS
    # -----------------------------------------------------------------------
    with TaskGroup("collecte") as tg_collecte:

        def collecter_donnees_ias(**context):
            """Télécharge les CSV IAS et retourne le chemin du JSON créé."""
            semaine = _get_semaine(context)
            annee, num_sem, base = _get_chemins(semaine)
            output_dir = "{}/raw".format(base)

            sys.path.insert(0, "/opt/airflow/scripts")
            from collecte_ias import (
                DATASETS_IAS,
                agreger_semaine,
                filtrer_semaine,
                sauvegarder_donnees,
                telecharger_csv_ias,
            )

            resultats = {}
            for syndrome, url in DATASETS_IAS.items():
                rows_all = telecharger_csv_ias(url)
                rows_sem = filtrer_semaine(rows_all, semaine)
                resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

            chemin = sauvegarder_donnees(resultats, semaine, output_dir)
            print("COLLECTE_OK:{}".format(chemin))
            return chemin

        collecter_sursaud = PythonOperator(
            task_id="collecter_donnees_sursaud",
            python_callable=collecter_donnees_ias,
            provide_context=True,
        )

    # -----------------------------------------------------------------------
    # Étape 5 — Archivage et vérification
    # -----------------------------------------------------------------------
    with TaskGroup("persistance_brute") as tg_archive:

        def archiver_local(**context):
            """Organise le fichier brut dans la structure partitionnée."""
            semaine = _get_semaine(context)
            annee, num_sem, base = _get_chemins(semaine)

            chemin_source = context["task_instance"].xcom_pull(
                task_ids="collecte.collecter_donnees_sursaud"
            )

            archive_dir = "{}/raw/{}/{}".format(base, annee, num_sem)
            os.makedirs(archive_dir, exist_ok=True)
            chemin_dest = "{}/sursaud_{}.json".format(archive_dir, semaine)
            shutil.copy2(chemin_source, chemin_dest)

            print("ARCHIVE_OK:{}".format(chemin_dest))
            return chemin_dest

        def verifier_archive(**context):
            """Vérifie que le fichier d'archive existe et n'est pas vide."""
            semaine = _get_semaine(context)
            annee, num_sem, base = _get_chemins(semaine)
            chemin = "{}/raw/{}/{}/sursaud_{}.json".format(base, annee, num_sem, semaine)

            if not os.path.exists(chemin):
                raise FileNotFoundError("Archive manquante : {}".format(chemin))
            taille = os.path.getsize(chemin)
            if taille == 0:
                raise ValueError("Archive vide : {}".format(chemin))

            print("ARCHIVE_VALIDE:{} ({} octets)".format(chemin, taille))
            return True

        archiver = PythonOperator(
            task_id="archiver_local",
            python_callable=archiver_local,
            provide_context=True,
        )
        verifier = PythonOperator(
            task_id="verifier_archive",
            python_callable=verifier_archive,
            provide_context=True,
        )

        archiver >> verifier

    # -----------------------------------------------------------------------
    # Étape 6 — Calcul des indicateurs
    # -----------------------------------------------------------------------
    with TaskGroup("traitement") as tg_traitement:

        def calculer_indicateurs_epidemiques(**context):
            """Calcule z-score, R0 et classification pour chaque syndrome."""
            semaine = _get_semaine(context)
            annee, num_sem, base = _get_chemins(semaine)

            chemin_brut = "{}/raw/{}/{}/sursaud_{}.json".format(base, annee, num_sem, semaine)
            with open(chemin_brut, encoding="utf-8") as f:
                donnees_brutes = json.load(f)

            sys.path.insert(0, "/opt/airflow/scripts")
            from calcul_indicateurs import calculer_indicateurs

            hook = PostgresHook(postgres_conn_id="postgres_ars")
            series_historiques = {}

            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT syndrome, valeur_ias
                        FROM donnees_hebdomadaires
                        WHERE semaine < %s
                        ORDER BY syndrome, annee DESC, numero_semaine DESC
                    """, (semaine,))
                    rows = cur.fetchall()

            tmp = defaultdict(list)
            for syndrome, valeur in rows:
                if len(tmp[syndrome]) < 4 and valeur is not None:
                    tmp[syndrome].append(valeur)

            for k, v in tmp.items():
                series_historiques[k] = list(reversed(v))

            seuil_alerte_z = float(Variable.get("seuil_alerte_zscore", default_var="1.5"))
            seuil_urgence_z = float(Variable.get("seuil_urgence_zscore", default_var="3.0"))

            indicateurs = []
            for syndrome, donnees in donnees_brutes["syndromes"].items():
                indicateur = calculer_indicateurs(
                    donnees,
                    series_historiques=series_historiques,
                    seuil_alerte_z=seuil_alerte_z,
                    seuil_urgence_z=seuil_urgence_z,
                )
                indicateurs.append(indicateur)

            output_dir = "{}/indicateurs/{}/{}".format(base, annee, num_sem)
            os.makedirs(output_dir, exist_ok=True)
            chemin_indic = "{}/indicateurs_{}.json".format(output_dir, semaine)
            with open(chemin_indic, "w", encoding="utf-8") as f:
                json.dump(indicateurs, f, ensure_ascii=False, indent=2)

            print("INDICATEURS_OK:{}".format(chemin_indic))
            return chemin_indic

        calculer_indic = PythonOperator(
            task_id="calculer_indicateurs_epidemiques",
            python_callable=calculer_indicateurs_epidemiques,
            provide_context=True,
        )

    # -----------------------------------------------------------------------
    # Étape 7 — Persistance PostgreSQL
    # -----------------------------------------------------------------------
    with TaskGroup("persistance_operationnelle") as tg_persistance:

        def inserer_donnees_postgres(**context):
            """Insère données hebdomadaires et indicateurs dans PostgreSQL."""
            semaine = _get_semaine(context)
            annee, num_sem, base = _get_chemins(semaine)

            chemin_brut = "{}/raw/{}/{}/sursaud_{}.json".format(base, annee, num_sem, semaine)
            chemin_indic = "{}/indicateurs/{}/{}/indicateurs_{}.json".format(base, annee, num_sem, semaine)

            with open(chemin_brut, encoding="utf-8") as f:
                donnees_brutes = json.load(f)
            with open(chemin_indic, encoding="utf-8") as f:
                indicateurs = json.load(f)

            hook = PostgresHook(postgres_conn_id="postgres_ars")

            sql_hbd = """
                INSERT INTO donnees_hebdomadaires
                    (semaine, syndrome, valeur_ias, seuil_min_saison,
                     seuil_max_saison, nb_jours_donnees)
                VALUES
                    (%(semaine)s, %(syndrome)s, %(valeur_ias)s,
                     %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
                ON CONFLICT (semaine, syndrome) DO UPDATE SET
                    valeur_ias       = EXCLUDED.valeur_ias,
                    seuil_min_saison = EXCLUDED.seuil_min_saison,
                    seuil_max_saison = EXCLUDED.seuil_max_saison,
                    nb_jours_donnees = EXCLUDED.nb_jours_donnees,
                    updated_at       = CURRENT_TIMESTAMP;
            """

            sql_indic = """
                INSERT INTO indicateurs_epidemiques
                    (semaine, syndrome, valeur_ias, z_score, r0_estime,
                     nb_saisons_reference, statut, statut_ias, statut_zscore, commentaire)
                VALUES
                    (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s,
                     %(r0_estime)s, %(nb_saisons_reference)s, %(statut)s,
                     %(statut_ias)s, %(statut_zscore)s, %(commentaire)s)
                ON CONFLICT (semaine, syndrome) DO UPDATE SET
                    valeur_ias           = EXCLUDED.valeur_ias,
                    z_score              = EXCLUDED.z_score,
                    r0_estime            = EXCLUDED.r0_estime,
                    nb_saisons_reference = EXCLUDED.nb_saisons_reference,
                    statut               = EXCLUDED.statut,
                    statut_ias           = EXCLUDED.statut_ias,
                    statut_zscore        = EXCLUDED.statut_zscore,
                    commentaire          = EXCLUDED.commentaire,
                    updated_at           = CURRENT_TIMESTAMP;
            """

            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    for syndrome, donnees in donnees_brutes["syndromes"].items():
                        if donnees.get("valeur_ias") is None:
                            logger.warning("Pas de valeur IAS pour {} — skip".format(syndrome))
                            continue
                        cur.execute(sql_hbd, donnees)
                    for indicateur in indicateurs:
                        if indicateur.get("valeur_ias") is None:
                            logger.warning("Pas d'indicateur pour {} — skip".format(indicateur.get("syndrome")))
                            continue
                        cur.execute(sql_indic, indicateur)
            conn.commit()

            logger.info("{} indicateurs insérés/mis à jour pour {}".format(len(indicateurs), semaine))

        inserer_postgres = PythonOperator(
            task_id="inserer_donnees_postgres",
            python_callable=inserer_donnees_postgres,
            provide_context=True,
        )

    # -----------------------------------------------------------------------
    # Étape 8 — Branchement épidémique
    # -----------------------------------------------------------------------

    def evaluer_situation_epidemique(**context):
        """Lit les indicateurs depuis PostgreSQL et choisit la branche."""
        semaine = _get_semaine(context)
        hook = PostgresHook(postgres_conn_id="postgres_ars")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT statut, COUNT(*) as nb
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    GROUP BY statut
                """, (semaine,))
                resultats = {row[0]: row[1] for row in cur.fetchall()}

        nb_urgence = resultats.get("URGENCE", 0)
        nb_alerte = resultats.get("ALERTE", 0)

        context["task_instance"].xcom_push(key="nb_urgence", value=nb_urgence)
        context["task_instance"].xcom_push(key="nb_alerte", value=nb_alerte)

        logger.info("Semaine {} — URGENCE: {}, ALERTE: {}".format(semaine, nb_urgence, nb_alerte))

        if nb_urgence > 0:
            return "declencher_alerte_ars"
        elif nb_alerte > 0:
            return "envoyer_bulletin_surveillance"
        else:
            return "confirmer_situation_normale"

    def declencher_alerte_ars(**context):
        semaine = _get_semaine(context)
        nb = context["task_instance"].xcom_pull(
            task_ids="evaluer_situation_epidemique", key="nb_urgence"
        )
        logger.critical("ALERTE ARS — Semaine {} — {} syndromes en URGENCE".format(semaine, nb))

    def envoyer_bulletin_surveillance(**context):
        semaine = _get_semaine(context)
        nb = context["task_instance"].xcom_pull(
            task_ids="evaluer_situation_epidemique", key="nb_alerte"
        )
        logger.warning("Bulletin surveillance — Semaine {} — {} syndromes en ALERTE".format(semaine, nb))

    def confirmer_situation_normale(**context):
        semaine = _get_semaine(context)
        logger.info("Situation normale — Semaine {} — aucune action requise".format(semaine))

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        provide_context=True,
    )
    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
        provide_context=True,
    )
    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
        provide_context=True,
    )
    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
        provide_context=True,
    )

    # -----------------------------------------------------------------------
    # Étape 9 — Rapport hebdomadaire
    # -----------------------------------------------------------------------

    def generer_rapport_hebdomadaire(**context):
        """Génère le rapport JSON, le sauvegarde et l'insère en base."""
        semaine = _get_semaine(context)
        annee, num_sem, base = _get_chemins(semaine)

        hook = PostgresHook(postgres_conn_id="postgres_ars")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT syndrome, valeur_ias, z_score,
                           r0_estime, statut, nb_saisons_reference
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    ORDER BY statut DESC, valeur_ias DESC NULLS LAST
                """, (semaine,))
                indicateurs = cur.fetchall()

        statuts = [row[4] for row in indicateurs]
        if "URGENCE" in statuts:
            situation_globale = "URGENCE"
        elif "ALERTE" in statuts:
            situation_globale = "ALERTE"
        else:
            situation_globale = "NORMAL"

        syndromes_urgence = [row[0] for row in indicateurs if row[4] == "URGENCE"]
        syndromes_alerte = [row[0] for row in indicateurs if row[4] == "ALERTE"]

        recommandations = {
            "URGENCE": [
                "Activation du plan de réponse épidémique régional",
                "Renforcement des équipes de surveillance dans les services d'urgences",
                "Notification immédiate à Santé Publique France et au Ministère de la Santé",
            ],
            "ALERTE": [
                "Surveillance renforcée des indicateurs pour les 48h suivantes",
                "Envoi d'un bulletin de surveillance aux partenaires de santé",
            ],
            "NORMAL": [
                "Maintien de la surveillance standard",
                "Prochain point épidémiologique dans 7 jours",
            ],
        }

        rapport = {
            "semaine": semaine,
            "region": "Occitanie",
            "code_region": "76",
            "date_generation": datetime.utcnow().isoformat(),
            "situation_globale": situation_globale,
            "syndromes_en_urgence": syndromes_urgence,
            "syndromes_en_alerte": syndromes_alerte,
            "indicateurs": [
                {
                    "syndrome": row[0],
                    "valeur_ias": row[1],
                    "z_score": row[2],
                    "r0_estime": row[3],
                    "statut": row[4],
                    "nb_saisons_reference": row[5],
                }
                for row in indicateurs
            ],
            "recommandations": recommandations[situation_globale],
            "genere_par": "ars_epidemio_dag v1.0",
            "pipeline_version": "2.8",
        }

        local_dir = "{}/rapports/{}/{}".format(base, annee, num_sem)
        local_path = "{}/rapport_{}.json".format(local_dir, semaine)
        os.makedirs(local_dir, exist_ok=True)
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(rapport, f, ensure_ascii=False, indent=2)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO rapports_ars
                        (semaine, situation_globale, nb_depts_alerte,
                         nb_depts_urgence, rapport_json, chemin_local)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (semaine) DO UPDATE SET
                        situation_globale = EXCLUDED.situation_globale,
                        nb_depts_alerte   = EXCLUDED.nb_depts_alerte,
                        nb_depts_urgence  = EXCLUDED.nb_depts_urgence,
                        rapport_json      = EXCLUDED.rapport_json,
                        chemin_local      = EXCLUDED.chemin_local,
                        updated_at        = CURRENT_TIMESTAMP
                """, (
                    semaine,
                    situation_globale,
                    len(syndromes_alerte),
                    len(syndromes_urgence),
                    json.dumps(rapport, ensure_ascii=False),
                    local_path,
                ))
            conn.commit()

        logger.info("Rapport {} généré — Statut : {}".format(semaine, situation_globale))

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # Dépendances
    # -----------------------------------------------------------------------
    (
        init_base_donnees
        >> tg_collecte
        >> tg_archive
        >> tg_traitement
        >> tg_persistance
        >> evaluer
        >> [alerte_ars, bulletin, normale]
        >> generer_rapport
    )