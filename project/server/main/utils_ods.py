"""
utils_ods.py
============
Téléchargement des jeux de données de l'open data ESR
(data.enseignementsup-recherche.gouv.fr, plateforme Opendatasoft).

    from .utils_ods import get_ods_data
    df = get_ods_data("fr-esr-principaux-etablissements-enseignement-superieur")

La clé API (variable d'environnement ODS_API_KEY) est envoyée dans un en-tête
et non dans l'URL, pour qu'elle n'apparaisse pas dans les traces d'erreur.
"""

import io

import pandas as pd
import requests
from retry import retry

from .config import ODS_API_KEY, ODS_BASE_URL
from .logger import get_logger

logger = get_logger(__name__)


class _ErreurTemporaire(requests.HTTPError):
    """HTTP 429 ou 5xx : vaut la peine de réessayer."""


@retry(exceptions=(requests.ConnectionError, requests.Timeout, _ErreurTemporaire),
       delay=10, backoff=2, tries=5, logger=logger)
def _fetch_ods_csv(key: str) -> bytes:
    """Contenu CSV brut du jeu `key`. Erreur réseau, 429 ou 5xx : nouvelle
    tentative (10 s, 20 s, 40 s, 80 s). Autre 4xx (jeu inconnu, clé refusée) :
    échec immédiat."""
    r = requests.get(
        f"{ODS_BASE_URL}/explore/dataset/{key}/download/",
        params={"format": "csv"},
        headers={"Authorization": f"Apikey {ODS_API_KEY}"} if ODS_API_KEY else {},
        timeout=300,
    )
    if r.status_code == 429 or r.status_code >= 500:
        raise _ErreurTemporaire(f"HTTP {r.status_code}", response=r)
    r.raise_for_status()
    return r.content


def get_ods_data(key: str, strict: bool = False, **read_csv_kwargs) -> pd.DataFrame:
    """DataFrame du jeu de données ODS `key` (séparateur « ; »).

    Une fois les tentatives épuisées : lève l'erreur si strict=True, sinon la
    journalise et renvoie un DataFrame vide. Les autres arguments sont passés
    à pd.read_csv (ex. dtype=str)."""
    logger.debug(f"getting ods data {key}")
    try:
        content = _fetch_ods_csv(key)
    except requests.RequestException as e:
        if strict:
            raise
        logger.error(f"ODS download failed for {key}: {e}")
        return pd.DataFrame([])
    return pd.read_csv(io.BytesIO(content), sep=";", **read_csv_kwargs)
