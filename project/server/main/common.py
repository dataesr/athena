"""
common.py
=========
Outils partagés par toutes les étapes : connexion DuckDB, périmètre de référence, pays
(noms, outre-mer sous FR), petits utilitaires de nettoyage (texte, valeur la plus fréquente)
et téléchargement avec copie locale.
"""

import shutil
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

from .config import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, PAYS_FR


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


def nom_fichier(nomenclature: str, perimetre: str = None) -> str:
    """Nom des sorties : <nomenclature>_<perimetre> (ex. panel_erc_revues_sjr), ou
    <nomenclature> pour une sortie qui ne dépend pas du périmètre."""
    return f"{nomenclature}_{perimetre}" if perimetre else nomenclature


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


def pays_sql(expr: str) -> str:
    """SQL : code pays OpenAlex -> code de l'entité pays ; outre-mer regroupé sous FR
    (PAYS_FR : 'RE' -> 'FR', 'GP' -> 'FR'...), autres codes inchangés. Même règle partout
    (articles, signatures des viviers, financeurs des awards...)."""
    return f"CASE WHEN {expr} IN ({_sql_in(PAYS_FR)}) THEN 'FR' ELSE {expr} END"


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


# --- Utilitaires communs (biblio, ERC, ANR, prix, Horizon) ---------------------------------

def texte(v, strict: bool = False):
    """Texte nettoyé (espaces de bord retirés), None si vide ou manquant.
    strict=True : seules les chaînes sont gardées (un nombre lu dans un JSON donne None)."""
    if strict:
        return v.strip() if isinstance(v, str) and v.strip() else None
    v = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    return v or None


def plus_frequent(s: pd.Series, ignorer_vides: bool = False):
    """Valeur la plus fréquente (manquants ignorés ; chaînes vides aussi si ignorer_vides),
    None s'il n'y en a pas. À égalité : ordre de value_counts."""
    s = s.dropna()
    if ignorer_vides:
        s = s[s.astype(str).str.strip() != ""]
    return s.value_counts().index[0] if len(s) else None


def pays_entite(s: pd.Series) -> pd.Series:
    """Code pays -> code de l'entité pays (pandas) : outre-mer regroupé sous FR (PAYS_FR), comme
    pays_sql en SQL."""
    return s.where(~s.isin(PAYS_FR), "FR")


def telecharger(url: str, f, refresh: bool = False, verbose: bool = True) -> Path:
    """Copie locale f de url (écriture atomique) ; gardée si elle existe et refresh=False."""
    f = Path(f)
    if f.exists() and not refresh:
        return f
    f.parent.mkdir(parents=True, exist_ok=True)
    tmp = f.with_suffix(f.suffix + ".tmp")
    with urllib.request.urlopen(url) as r, open(tmp, "wb") as out:
        shutil.copyfileobj(r, out)
    tmp.replace(f)
    if verbose:
        print(f"Téléchargé : {url.rsplit('/', 1)[-1]} -> {f} ({f.stat().st_size / 1e6:.0f} Mo)")
    return f
