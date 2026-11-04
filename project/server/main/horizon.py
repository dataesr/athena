"""
horizon.py
==========
Participations aux projets Horizon Europe (open data ESR), pour les ERC (erc.py) et,
plus tard, d'autres programmes.

Sources
-------
  Deux jeux de même structure, une ligne par participation (projet x structure x rôle) :
    soumis    HORIZON_ODS_SOUMIS   (fr-esr-horizon-projets-soumis-candidats)
              propositions évaluées, structure au dépôt (stage = evaluated)
    laureats  HORIZON_ODS_LAUREATS (fr-esr-horizon-projects-entities)
              projets financés, structure à la signature (stage = successful)
  Le jeu des soumis n'est pas public : clé ODS_API_KEY en variable d'environnement.

Fonctions
---------
  lire_horizon(jeu)          jeu brut en texte, copié dans <HORIZON_DIR>/horizon_<jeu>.parquet
                             (telecharger=False : relit la copie)
  participations(df, jeu)    colonnes normalisées (une ligne par participation) :
      jeu, project_id, participation_id, stage, annee_appel, pilier_code, programme_code,
      thema_code, action_id, destination_code (en majuscules : SyG -> SYG), panel_code,
      role, erc_role, participates_as, entities_id (un identifiant par ligne : Paysage,
      ROR ou SIREN...), entities_id_source, entities_name, pays_code (ISO2, comme OpenAlex :
      GB et GR ; outre-mer sous son code propre, ex. RE), rnsr (numero_national_de_structure :
      laboratoires RNSR de la participation française, séparés par ;)
  rattacher_structures(p)    établissements (ROR) de chaque participation, logique de
                             scanR (scanr-backend, projects.get_participations_simple) :
      - la structure participante (entities_id : Paysage, ROR ou RNSR), remplacée par son
        successeur Paysage si elle a été fusionnée ou renommée (structure actuelle) ;
      - pour chaque laboratoire RNSR (numero_national_de_structure, ou entities_id RNSR),
        toutes ses tutelles (scanR, « établissement tutelle »), chacune remplacée par son
        successeur Paysage : le projet compte aussi pour les universités et écoles
        tutelles du laboratoire, pas seulement pour l'organisme bénéficiaire (CNRS...).

Exemple :
    from project.server.main.horizon import lire_horizon, participations
    p = participations(lire_horizon("laureats"), "laureats")
"""

import re
from pathlib import Path

import duckdb
import pandas as pd

from .config import (HORIZON_COLONNES_EXCLUES, HORIZON_DIR, HORIZON_ODS_LAUREATS,
                     HORIZON_ODS_SOUMIS, REFERENTIEL_DIR)
from .referentiel import (charger_correspondance_paysage, charger_successeurs_paysage,
                          charger_tutelles_rnsr, successeur_final)
from .utils_ods import get_ods_data

JEUX = {"soumis": HORIZON_ODS_SOUMIS, "laureats": HORIZON_ODS_LAUREATS}

# ROR court : 0 + 6 caractères (alphabet Crockford, sans i, l, o, u) + 2 chiffres
ROR_RE = re.compile(r"^0[a-hj-km-np-tv-z0-9]{6}[0-9]{2}$")
# Numéro RNSR : année (4 chiffres) + 5 chiffres + lettre de contrôle, ex. 199719338M
RNSR_RE = re.compile(r"^[0-9]{9}[A-Z]$")

COLONNES_REQUISES = ["project_id", "call_year", "destination_code", "entities_id",
                     "entities_name", "country_code"]

# Codes « européens » de la Commission -> ISO 3166-1 alpha-2
CORRECTIONS_ISO2 = {"UK": "GB", "EL": "GR"}
# ISO3 -> ISO2 (pays des participants : UE, pays associés et autres)
ISO3_ISO2 = {
    "AUT": "AT", "BEL": "BE", "BGR": "BG", "HRV": "HR", "CYP": "CY", "CZE": "CZ", "DNK": "DK",
    "EST": "EE", "FIN": "FI", "FRA": "FR", "DEU": "DE", "GRC": "GR", "HUN": "HU", "IRL": "IE",
    "ITA": "IT", "LVA": "LV", "LTU": "LT", "LUX": "LU", "MLT": "MT", "NLD": "NL", "POL": "PL",
    "PRT": "PT", "ROU": "RO", "SVK": "SK", "SVN": "SI", "ESP": "ES", "SWE": "SE",
    "GBR": "GB", "CHE": "CH", "NOR": "NO", "ISL": "IS", "LIE": "LI", "ISR": "IL", "TUR": "TR",
    "SRB": "RS", "MNE": "ME", "MKD": "MK", "ALB": "AL", "BIH": "BA", "MDA": "MD", "UKR": "UA",
    "GEO": "GE", "ARM": "AM", "FRO": "FO", "TUN": "TN", "XKX": "XK", "KOS": "XK", "RUS": "RU",
    "USA": "US", "CAN": "CA", "MEX": "MX", "BRA": "BR", "ARG": "AR", "CHL": "CL", "COL": "CO",
    "CHN": "CN", "JPN": "JP", "KOR": "KR", "TWN": "TW", "HKG": "HK", "SGP": "SG", "IND": "IN",
    "AUS": "AU", "NZL": "NZ", "ZAF": "ZA", "EGY": "EG", "MAR": "MA", "DZA": "DZ",
    "CMR": "CM", "KEN": "KE", "UGA": "UG", "SEN": "SN", "NGA": "NG", "GHA": "GH", "ETH": "ET",
    "GLP": "GP", "MTQ": "MQ", "GUF": "GF", "REU": "RE", "MYT": "YT", "SPM": "PM", "BLM": "BL",
    "MAF": "MF", "WLF": "WF", "PYF": "PF", "NCL": "NC",
}


def horizon_path(jeu: str, horizon_dir: str = HORIZON_DIR) -> Path:
    return Path(horizon_dir) / f"horizon_{jeu}.parquet"


def lire_horizon(jeu: str, horizon_dir: str = HORIZON_DIR, telecharger: bool = True,
                 colonnes_exclues=HORIZON_COLONNES_EXCLUES) -> pd.DataFrame:
    """Jeu `jeu` ("soumis" ou "laureats") en texte (aucune conversion automatique),
    sans les colonnes `colonnes_exclues`, copié dans <horizon_dir>/horizon_<jeu>.parquet.
    telecharger=False : relit cette copie."""
    if jeu not in JEUX:
        raise ValueError(f"jeu inconnu : {jeu} (attendu : {list(JEUX)})")
    f = horizon_path(jeu, horizon_dir)
    if not telecharger:
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : relancer avec telecharger=True")
        return duckdb.sql(f"SELECT * FROM read_parquet('{f}')").df()
    exclues = {c.lower() for c in (colonnes_exclues or [])}
    df = get_ods_data(JEUX[jeu], strict=True, dtype=str, keep_default_na=False,
                      usecols=lambda c: c.lstrip("\ufeff").strip().lower() not in exclues)
    if df.empty:
        raise ValueError(f"jeu ODS {JEUX[jeu]} vide")
    df.columns = [c.lstrip("\ufeff").strip().lower() for c in df.columns]   # BOM éventuel
    f.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY df TO '{f}' (FORMAT parquet, COMPRESSION zstd)")
    return df


def pays_iso2(iso3: str, code2: str = None):
    """Code ISO2 du pays : par l'ISO3, sinon par un code à deux lettres de la
    Commission (UK -> GB, EL -> GR). None si inconnu."""
    iso3 = (iso3 or "").strip().upper()
    if iso3 in ISO3_ISO2:
        return ISO3_ISO2[iso3]
    for c in (iso3, (code2 or "").strip().upper()):
        if len(c) == 2 and c.isalpha():
            return CORRECTIONS_ISO2.get(c, c)
    return None


def _texte(v):
    v = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    return v or None


def participations(df: pd.DataFrame, jeu: str) -> pd.DataFrame:
    """Colonnes normalisées (voir l'en-tête). Une cellule entities_id à plusieurs
    identifiants (séparés par ; ou |) donne une ligne par identifiant."""
    manquantes = [c for c in COLONNES_REQUISES if c not in df.columns]
    if manquantes:
        raise ValueError(f"colonnes absentes du jeu {jeu} : {manquantes} (lues : {list(df.columns)})")
    col = lambda c: (df[c].map(_texte) if c in df.columns else pd.Series([None] * len(df), index=df.index))
    p = pd.DataFrame({
        "jeu": jeu,
        "project_id": col("project_id"),
        "participation_id": col("participation_linked"),
        "stage": col("stage").str.lower(),
        "annee_appel": pd.to_numeric(col("call_year"), errors="coerce").astype("Int64"),
        "pilier_code": col("pilier_code"),
        "programme_code": col("programme_code"),
        "thema_code": col("thema_code").str.upper(),
        "action_id": col("action_id").str.upper(),
        "destination_code": col("destination_code").str.upper(),
        "panel_code": col("panel_code").str.upper(),
        "role": col("role"),
        "erc_role": col("erc_role"),
        "participates_as": col("participates_as"),
        "entities_id": col("entities_id"),
        "entities_id_source": col("entities_id_source"),
        "entities_name": col("entities_name"),
        "rnsr": col("numero_national_de_structure"),
        "pays_code": [pays_iso2(a, b) for a, b in zip(
            col("country_code"), col("country_code2") if "country_code2" in df.columns
            else [None] * len(df))],
    })
    p = p[p["project_id"].notna()]
    # identifiant de participation : celui de la source, sinon projet + structure + rang
    rang = p.groupby(["project_id", "entities_id"], dropna=False).cumcount().astype(str)
    p["participation_id"] = p["participation_id"].fillna(
        p["project_id"] + "|" + p["entities_id"].fillna("") + "|" + rang)
    p = p.assign(entities_id=p["entities_id"].str.split(r"[;|]")).explode("entities_id")
    p["entities_id"] = p["entities_id"].map(_texte)
    return p.reset_index(drop=True)


def _rnsrs(v) -> list:
    if not isinstance(v, str):
        return []
    return [x for x in (t.strip().upper() for t in re.split(r"[;|,]", v)) if RNSR_RE.match(x)]


def rattacher_structures(p: pd.DataFrame, ref_dir: str = REFERENTIEL_DIR, successeurs: bool = True,
                         tutelles_rnsr: bool = True) -> tuple:
    """(p, liens) :
    p      : participations + ligne (numéro de ligne), type_identifiant (ror | paysage | rnsr |
             inconnu | vide), ror (ROR actuel de la structure participante, ou None),
             successeur (vrai si la structure a été remplacée par son successeur),
             nb_tutelles_rnsr (tutelles trouvées par le RNSR, avec un ROR)
    liens  : (ligne, ror, origine) : un lien par établissement auquel la participation
             est rattachée ; origine = structure | tutelle_rnsr"""
    p = p.reset_index(drop=True).copy()
    p["ligne"] = range(len(p))
    corresp = charger_correspondance_paysage(ref_dir)
    p2r = dict(zip(corresp["referentiel_id"], corresp["ror"]))
    r2p_tous = corresp.groupby("ror")["referentiel_id"].agg(list)
    r2p = {r: v[0] for r, v in r2p_tous.items() if len(v) == 1}   # ROR -> Paysage, si sans ambiguïté
    succ = charger_successeurs_paysage(ref_dir) if successeurs else {}
    tut = charger_tutelles_rnsr(ref_dir) if tutelles_rnsr else pd.DataFrame(columns=["rnsr", "tutelle_id"])
    tutelles = tut.groupby("rnsr")["tutelle_id"].agg(list).to_dict()

    def ror_actuel(pid):
        """ROR de la structure Paysage actuelle (successeur), à défaut celui de pid."""
        fin = successeur_final(pid, succ)
        return p2r.get(fin) or p2r.get(pid), fin != pid

    rors, types, remplacees, liens, nb_tut = [], [], [], [], []
    for ligne, i, rn in zip(p["ligne"], p["entities_id"].fillna(""), p["rnsr"]):
        norm = i.strip().rstrip("/").split("/")[-1]
        ror, remplace, labos = None, False, _rnsrs(rn)
        if not i:
            t = "vide"
        elif ROR_RE.match(norm.lower()):
            t, ror = "ror", norm.lower()
            if ror in r2p:
                autre, remplace = ror_actuel(r2p[ror])
                ror = autre if remplace and autre else ror
                remplace = remplace and bool(autre)
        elif i in p2r or i in succ:
            t = "paysage"
            ror, remplace = ror_actuel(i)
        elif RNSR_RE.match(i.strip().upper()):
            t = "rnsr"
            labos.append(i.strip().upper())
        else:
            t = "inconnu"
        rors.append(ror)
        types.append(t)
        remplacees.append(bool(remplace and ror))
        if ror:
            liens.append((ligne, ror, "structure"))
        n = 0
        for labo in dict.fromkeys(labos):
            for tid in tutelles.get(labo, []):
                r, _ = ror_actuel(tid)
                if r:
                    liens.append((ligne, r, "tutelle_rnsr"))
                    n += 1
        nb_tut.append(n)
    p["type_identifiant"], p["ror"], p["successeur"], p["nb_tutelles_rnsr"] = types, rors, remplacees, nb_tut
    liens = pd.DataFrame(liens, columns=["ligne", "ror", "origine"]).drop_duplicates(["ligne", "ror"])
    return p, liens
