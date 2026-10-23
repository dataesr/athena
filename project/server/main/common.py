"""
common.py
=========
Outils partagés par les étapes 3 à 7 : connexion DuckDB, périmètre de
référence, noms des pays.
"""

from pathlib import Path

import duckdb

from .config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS


def connect(data_dir: str, memory_limit: str = DUCKDB_MEMORY_LIMIT,
            threads: int = DUCKDB_THREADS) -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB locale ; les tris trop gros pour la RAM débordent
    sur disque dans <data_dir>/tmp_duckdb."""
    con = duckdb.connect()
    tmp = Path(data_dir) / "tmp_duckdb"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmp}'")
    con.execute("SET preserve_insertion_order = false")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    return con


def _sql_in(values) -> str:
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


# Valeurs possibles du périmètre de référence (voir config.PERIMETRE)
PERIMETRES_VALIDES = ("toutes_revues_openalex", "revues_sjr")
# Identifiant de l'entité « monde » dans les effectifs (dénominateur des parts)
MONDE_ID = "MONDE"


def sources_sjr_path(data_dir: str) -> Path:
    return Path(data_dir) / "revues_tete" / "sources_sjr.parquet"


def perimetre_sources(perimetre: str, data_dir: str):
    """Fichier des sources autorisées pour un périmètre, ou None (pas de filtre)."""
    if perimetre not in PERIMETRES_VALIDES:
        raise ValueError(f"périmètre inconnu : {perimetre!r} (attendu : {PERIMETRES_VALIDES})")
    if perimetre == "toutes_revues_openalex":
        return None
    p = sources_sjr_path(data_dir)
    if not p.exists():
        raise FileNotFoundError(f"{p} absent : lancer d'abord l'étape 5 (compute_top_journals)")
    return str(p)


def nom_fichier(classement: str, perimetre: str = None) -> str:
    """Nom des sorties : <classement>_<perimetre> (ex. erc_revues_sjr), ou
    <classement> pour une sortie qui ne dépend pas du périmètre."""
    return f"{classement}_{perimetre}" if perimetre else classement


def perimeter_where(work_types=None, source_types=None, annee_min=None,
                    annee_max=None, alias: str = "w", sources_file: str = None) -> str:
    """Clause WHERE du périmètre ; un paramètre à None n'est pas filtré.
    sources_file : parquet avec une colonne source_id (ex. revues SJR)."""
    cond = ["TRUE"]
    if sources_file:
        cond.append(f"{alias}.source_id IN (SELECT source_id FROM read_parquet('{sources_file}'))")
    if work_types:
        cond.append(f"{alias}.type IN ({_sql_in(work_types)})")
    if source_types:
        cond.append(f"{alias}.source_type IN ({_sql_in(source_types)})")
    if annee_min is not None:
        cond.append(f"{alias}.year >= {int(annee_min)}")
    if annee_max is not None:
        cond.append(f"{alias}.year <= {int(annee_max)}")
    return " AND ".join(cond)


def pays_list_sql(pays) -> str:
    """Liste SQL de codes pays : ('FR','GP',...) -> 'FR', 'GP', ..."""
    return _sql_in(pays)


# Noms français des pays (codes ISO 3166-1 alpha-2 utilisés par OpenAlex)
PAYS_NOMS = {
    "FR": "France", "US": "États-Unis", "CN": "Chine", "GB": "Royaume-Uni", "DE": "Allemagne",
    "JP": "Japon", "IT": "Italie", "CA": "Canada", "IN": "Inde", "ES": "Espagne",
    "AU": "Australie", "KR": "Corée du Sud", "BR": "Brésil", "NL": "Pays-Bas", "RU": "Russie",
    "CH": "Suisse", "SE": "Suède", "TR": "Turquie", "IR": "Iran", "PL": "Pologne",
    "BE": "Belgique", "TW": "Taïwan", "DK": "Danemark", "ID": "Indonésie", "MX": "Mexique",
    "AT": "Autriche", "NO": "Norvège", "PT": "Portugal", "SA": "Arabie saoudite", "IL": "Israël",
    "FI": "Finlande", "ZA": "Afrique du Sud", "EG": "Égypte", "PK": "Pakistan", "SG": "Singapour",
    "MY": "Malaisie", "CZ": "Tchéquie", "GR": "Grèce", "IE": "Irlande", "NZ": "Nouvelle-Zélande",
    "AR": "Argentine", "CL": "Chili", "CO": "Colombie", "TH": "Thaïlande", "NG": "Nigeria",
    "HK": "Hong Kong", "UA": "Ukraine", "RO": "Roumanie", "HU": "Hongrie", "VN": "Viêt Nam",
    "BD": "Bangladesh", "MA": "Maroc", "TN": "Tunisie", "DZ": "Algérie", "IQ": "Irak",
    "AE": "Émirats arabes unis", "QA": "Qatar", "LU": "Luxembourg", "SK": "Slovaquie",
    "SI": "Slovénie", "HR": "Croatie", "RS": "Serbie", "BG": "Bulgarie", "EE": "Estonie",
    "LT": "Lituanie", "LV": "Lettonie", "IS": "Islande", "CY": "Chypre", "MT": "Malte",
    "PE": "Pérou", "EC": "Équateur", "KE": "Kenya", "ET": "Éthiopie", "GH": "Ghana",
    "PH": "Philippines", "KZ": "Kazakhstan", "JO": "Jordanie", "LB": "Liban",
}
# Grands domaines (lettres en tête des codes de discipline ERC)
DOMAINES_NOMS = {"LS": "Sciences de la vie", "PE": "Sciences physiques et ingénierie",
                 "SH": "Sciences humaines et sociales"}

# Pays de l'Union européenne (rang « parmi les pays européens »)
PAYS_UE = ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE",
           "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]

# Grandes régions (couleurs des graphiques de comparaison). Les pays absents
# de cette liste sont classés en « Afrique, Moyen-Orient et autres ».
REGIONS = {
    "europe": ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "DE", "GR", "HU", "IE", "IT",
               "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "GB", "CH",
               "NO", "IS", "UA", "RS", "RU", "TR"],
    "ameriques": ["US", "CA", "MX", "BR", "AR", "CL", "CO", "PE", "EC"],
    "asie": ["CN", "JP", "KR", "TW", "HK", "SG", "IN", "ID", "MY", "TH", "VN", "PH", "PK", "BD",
             "KZ", "AU", "NZ"],
}
