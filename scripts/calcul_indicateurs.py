#!/usr/bin/env python3
"""
Calcul des indicateurs épidémiques IAS® — ARS Occitanie
"""

import json
import logging
import sys
from typing import Optional, List, Dict

import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SAISONS_COLS = [
    "Sais_2023_2024",
    "Sais_2022_2023",
    "Sais_2021_2022",
    "Sais_2020_2021",
    "Sais_2019_2020",
]

DUREE_INFECTIEUSE = {
    "GRIPPE":   5,
    "GEA":      3,
    "SG":       5,
    "BRONCHIO": 7,
    "COVID19":  7,
}


def calculer_zscore(valeur_actuelle, historique):
    # type: (float, List) -> Optional[float]
    """Calcule le z-score par rapport aux saisons historiques."""
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning("Historique insuffisant ({} saisons)".format(len(valeurs_valides)))
        return None

    moyenne = np.mean(valeurs_valides)
    ecart_type = np.std(valeurs_valides, ddof=1)

    if ecart_type == 0:
        return 0.0

    return float((valeur_actuelle - moyenne) / ecart_type)


def classifier_statut_ias(valeur_ias, seuil_min, seuil_max):
    # type: (float, Optional[float], Optional[float]) -> str
    """Classifie selon les seuils MIN/MAX du dataset IAS."""
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(z_score, seuil_alerte_z=1.5, seuil_urgence_z=3.0):
    # type: (Optional[float], float, float) -> str
    """Classifie selon le z-score."""
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias, statut_zscore):
    # type: (str, str) -> str
    """Retient le niveau le plus sévère entre les deux critères."""
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(series_hebdomadaire, duree_infectieuse=5):
    # type: (List, int) -> Optional[float]
    """Estimation du R0 par taux de croissance moyen."""
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]
    if len(series_valides) < 2:
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
    ]

    if not croissances:
        return None

    return max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7)))


def calculer_indicateurs(donnees_semaine, series_historiques, seuil_alerte_z=1.5, seuil_urgence_z=3.0):
    # type: (Dict, Dict, float, float) -> Dict
    """
    Calcule l'ensemble des indicateurs pour un syndrome donné.
    donnees_semaine : dict issu de agreger_semaine()
    series_historiques : {syndrome: [val_s-4, ..., val_s-1]}
    """
    syndrome = donnees_semaine["syndrome"]
    valeur_ias = donnees_semaine.get("valeur_ias")

    if valeur_ias is None:
        logger.warning("Pas de valeur IAS pour {} — indicateurs non calculés".format(syndrome))
        return {
            "semaine":              donnees_semaine["semaine"],
            "syndrome":             syndrome,
            "valeur_ias":           None,
            "z_score":              None,
            "r0_estime":            None,
            "nb_saisons_reference": 0,
            "statut":               "NORMAL",
            "statut_ias":           "NORMAL",
            "statut_zscore":        "NORMAL",
            "commentaire":          "Donnees IAS indisponibles pour cette semaine",
        }

    historique_vals = [
        donnees_semaine["historique"].get(col)
        for col in SAISONS_COLS
    ]
    z_score = calculer_zscore(valeur_ias, historique_vals)
    nb_saisons = len([v for v in historique_vals if v is not None])

    series = series_historiques.get(syndrome, [])
    duree = DUREE_INFECTIEUSE.get(syndrome, 5)
    r0 = calculer_r0_simplifie(series + [valeur_ias], duree_infectieuse=duree)

    statut_ias = classifier_statut_ias(
        valeur_ias,
        donnees_semaine.get("seuil_min"),
        donnees_semaine.get("seuil_max"),
    )
    statut_z = classifier_statut_zscore(z_score, seuil_alerte_z, seuil_urgence_z)
    statut_final = classifier_statut_final(statut_ias, statut_z)

    return {
        "semaine":              donnees_semaine["semaine"],
        "syndrome":             syndrome,
        "valeur_ias":           round(valeur_ias, 3),
        "z_score":              round(z_score, 3) if z_score is not None else None,
        "r0_estime":            round(r0, 3) if r0 is not None else None,
        "nb_saisons_reference": nb_saisons,
        "statut":               statut_final,
        "statut_ias":           statut_ias,
        "statut_zscore":        statut_z,
        "commentaire":          "Calcule sur {} saisons historiques".format(nb_saisons),
    }


if __name__ == "__main__":
    chemin_json = sys.argv[1] if len(sys.argv) > 1 else "/data/ars/raw/ias_test.json"

    with open(chemin_json, encoding="utf-8") as f:
        data = json.load(f)

    for syndrome, donnees in data["syndromes"].items():
        indicateur = calculer_indicateurs(donnees, series_historiques={})
        print(json.dumps(indicateur, ensure_ascii=False, indent=2))