"""
erc.py
======
Bourses ERC : candidatures et lauréats par établissement, pays et panel, pour
l'onglet ERC du dashboard. Mêmes clés que la bibliométrie (CONTRAT_DONNEES.md) :
entite_id (ROR court, code ISO du pays, MONDE) et discipline_code (panel ERC :
LS1…LS9, PE1…PE11, SH1…SH8), pour partager les filtres (établissement français,
discipline, comparaison internationale).

Source
------
  Open data ESR, jeu ERC_ODS_DATASET (fr-esr-erc-persons) : une ligne par personne
  et par projet (role : fellow, principal investigator, main_contact...), avec
  stage = evaluated (candidature évaluée) ou successful (projet financé).
  Colonnes utilisées : project_id, stage, call_year, panel_code, destination_code
  (STG, COG, ADG, SYG, POC), entities_id (identifiant Paysage ou ROR de la structure
  d'accueil), entities_name, country_code (ISO3), country_code2.

Règles
------
  Candidature : un project_id. Lauréat : un projet qui a au moins une ligne
  stage = ERC_STAGE_LAUREAT. Le taux de succès d'une entité est donc
  nb_candidatures_retenues / nb_candidatures, le numérateur étant inclus dans
  le dénominateur.
  Structure d'accueil : celle des lignes de candidature (stage <> successful) ; à
  défaut (projet présent seulement comme lauréat), celle des lignes du lauréat. Un
  PI qui change d'établissement entre le dépôt et la signature reste compté dans
  l'établissement où il a candidaté.
  Établissement : entities_id converti en ROR (directement, ou par la table
  Paysage -> ROR de referentiel.py). Si ERC_RATTACHEMENT_LIGNEE, la candidature
  compte aussi pour les tutelles OpenAlex du ROR (même règle que les articles :
  un ERC d'un institut compte pour l'institut et son université). Les structures
  sans ROR sont listées dans erc_non_rattaches.csv.
  Pays : pays de la structure d'accueil (country_code), en code ISO2 comme OpenAlex
  (GB et GR, pas UK et EL).
  Comptage entier : une candidature compte 1 pour chaque entité présente (une
  Synergy à plusieurs établissements compte pour chacun) ; MONDE = toutes les
  candidatures, une fois chacune.
  Une bourse ERC n'a qu'un porteur (sauf les Synergy, 2 à 4) : le nombre de
  candidatures est donc, à ces exceptions près, le nombre de personnes qui ont
  candidaté (une personne qui candidate deux années compte deux fois).

Sorties (dans ERC_DIR = <DATA_DIR>/erc/)
----------------------------------------
  erc_persons.parquet       copie brute du jeu ODS (relecture, reproductibilité)
  erc_effectifs.parquet     une ligne par entite_type x entite_id x annee_appel x
                            discipline_code x type_bourse :
                              nb_candidatures, nb_candidatures_retenues
  erc_entites.parquet       pays et établissements présents : entite_id, entite_type,
                            nom, pays_code, nb_candidatures, nb_candidatures_retenues
                            (sert à compléter la collection « entites », entites.py)
  erc_meta.json             paramètres, années, pays comparés, textes de méthode
  erc_non_rattaches.csv     structures d'accueil sans ROR (à corriger dans Paysage)

Exemple :
    from project.server.main.erc import compute_erc
    res = compute_erc()
    res = compute_erc(telecharger=False)   # réutilise erc_persons.parquet
"""

import datetime as dt
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .common import MONDE_ID, PAYS_NOMS, pays_list_sql
from .config import (CLASSEMENT, DATA_DIR, ERC_DIR, ERC_ODS_DATASET, ERC_PAYS_COMPARAISON,
                     ERC_PAYS_COMPARAISON_N, ERC_RATTACHEMENT_LIGNEE, ERC_STAGE_LAUREAT,
                     ERC_TYPES_BOURSE, ETAB_EXCLUDE, LINEAGE_CUTS, PAYS_FR, REFERENTIEL_DIR)
from .referentiel import charger_correspondance_paysage
from .utils_ods import get_ods_data

COLONNES_REQUISES = ["project_id", "stage", "call_year", "panel_code", "destination_code",
                     "entities_id", "entities_name", "country_code"]
CLES = ["annee_appel", "discipline_code", "type_bourse"]

# ROR court : 0 + 6 caractères (alphabet Crockford, sans i, l, o, u) + 2 chiffres
ROR_RE = re.compile(r"^0[a-hj-km-np-tv-z0-9]{6}[0-9]{2}$")

# Codes « européens » de la Commission -> ISO 3166-1 alpha-2
CORRECTIONS_ISO2 = {"UK": "GB", "EL": "GR"}
# ISO3 -> ISO2 (pays des structures d'accueil ERC : UE, pays associés et autres)
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
    "GLP": "GP", "MTQ": "MQ", "GUF": "GF", "REU": "RE", "MYT": "YT", "SPM": "PM", "BLM": "BL",
    "MAF": "MF", "WLF": "WF", "PYF": "PF", "NCL": "NC",
}


def erc_path(nom: str, erc_dir: str = ERC_DIR) -> Path:
    return Path(erc_dir) / nom


# --- Lecture -----------------------------------------------------------------------

def lire_erc(dataset: str = ERC_ODS_DATASET, erc_dir: str = ERC_DIR,
             telecharger: bool = True) -> pd.DataFrame:
    """Jeu ODS en texte (aucune conversion automatique), copié dans
    <erc_dir>/erc_persons.parquet. telecharger=False : relit cette copie."""
    f = erc_path("erc_persons.parquet", erc_dir)
    if not telecharger:
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : lancer compute_erc(telecharger=True)")
        return duckdb.sql(f"SELECT * FROM read_parquet('{f}')").df()
    df = get_ods_data(dataset, strict=True, dtype=str, keep_default_na=False)
    if df.empty:
        raise ValueError(f"jeu ODS {dataset} vide")
    df.columns = [c.strip().lower() for c in df.columns]
    f.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY df TO '{f}' (FORMAT parquet, COMPRESSION zstd)")
    return df


def pays_iso2(iso3: str, code2: str = None):
    """Code ISO2 du pays : par l'ISO3, sinon par le code à deux lettres de la
    Commission (UK -> GB, EL -> GR). None si inconnu."""
    iso3 = (iso3 or "").strip().upper()
    if iso3 in ISO3_ISO2:
        return ISO3_ISO2[iso3]
    c2 = (code2 or "").strip().upper()
    if len(c2) == 2 and c2.isalpha():
        return CORRECTIONS_ISO2.get(c2, c2)
    return None


def _texte(v):
    v = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    return v or None


def _plus_frequent(s: pd.Series):
    s = s.dropna()
    return s.value_counts().index[0] if len(s) else None


# --- Préparation ---------------------------------------------------------------------

def preparer_candidatures(df: pd.DataFrame, stage_laureat: str = ERC_STAGE_LAUREAT,
                          types_bourse=ERC_TYPES_BOURSE) -> tuple:
    """(projets, accueil) :
    projets : une ligne par project_id : annee_appel, discipline_code, type_bourse, laureat
    accueil : (project_id, entities_id, entities_name, pays_code) des structures d'accueil
              retenues (lignes de candidature, à défaut lignes du lauréat)"""
    manquantes = [c for c in COLONNES_REQUISES if c not in df.columns]
    if manquantes:
        raise ValueError(f"colonnes absentes du jeu ERC : {manquantes} (lues : {list(df.columns)})")
    d = pd.DataFrame({
        "project_id": df["project_id"].map(_texte),
        "stage": df["stage"].map(_texte).str.lower(),
        "annee_appel": pd.to_numeric(df["call_year"].map(_texte), errors="coerce").astype("Int64"),
        "discipline_code": df["panel_code"].map(_texte).str.upper(),
        "type_bourse": df["destination_code"].map(_texte).str.upper(),
        "entities_id": df["entities_id"].map(_texte),
        "entities_name": df["entities_name"].map(_texte),
        "pays_code": [pays_iso2(a, b) for a, b in zip(
            df["country_code"], df["country_code2"] if "country_code2" in df.columns else [None] * len(df))],
    })
    d = d[d["project_id"].notna()]
    if types_bourse:
        d = d[d["type_bourse"].isin([t.upper() for t in types_bourse])]
    d["est_laureat"] = d["stage"] == stage_laureat.lower()

    g = d.groupby("project_id")
    projets = pd.DataFrame({
        "annee_appel": g["annee_appel"].agg(_plus_frequent),
        "discipline_code": g["discipline_code"].agg(_plus_frequent),
        "type_bourse": g["type_bourse"].agg(_plus_frequent),
        "laureat": g["est_laureat"].any(),
        "a_candidature": (~d["est_laureat"]).groupby(d["project_id"]).any(),
    }).reset_index()
    projets["annee_appel"] = projets["annee_appel"].astype("Int64")

    # Lignes d'accueil : candidature si le projet en a, sinon lauréat
    avec_cand = set(projets.loc[projets["a_candidature"], "project_id"])
    garde = d["project_id"].isin(avec_cand) != d["est_laureat"]
    acc = d.loc[garde, ["project_id", "entities_id", "entities_name", "pays_code"]]
    # Plusieurs identifiants dans une cellule (séparés par ; | ,) : une ligne chacun
    acc = acc.assign(entities_id=acc["entities_id"].str.split(r"[;|,]")).explode("entities_id")
    acc["entities_id"] = acc["entities_id"].map(_texte)
    return projets.drop(columns="a_candidature"), acc.drop_duplicates()


def rattacher_ror(accueil: pd.DataFrame, ref_dir: str = REFERENTIEL_DIR) -> pd.DataFrame:
    """Ajoute ror et type_identifiant (ror | paysage | inconnu | vide) aux lignes d'accueil."""
    corresp = charger_correspondance_paysage(ref_dir)
    p2r = dict(zip(corresp["referentiel_id"], corresp["ror"]))
    ids = accueil["entities_id"].fillna("")
    norm = ids.str.strip().str.rstrip("/").str.split("/").str[-1]
    est_ror = norm.str.lower().str.match(ROR_RE)
    a = accueil.copy()
    a["ror"] = norm.str.lower().where(est_ror, ids.map(p2r))
    a["type_identifiant"] = ["vide" if not i else "ror" if r else "paysage" if i in p2r else "inconnu"
                             for i, r in zip(ids, est_ror)]
    return a


def tutelles_ror(rors, data_dir: str = DATA_DIR, lineage_cuts=LINEAGE_CUTS,
                 etab_exclude=ETAB_EXCLUDE) -> pd.DataFrame:
    """(ror, entite_id) : chaque ROR et les ROR de ses tutelles (lignée OpenAlex
    corrigée par LINEAGE_CUTS), moins ETAB_EXCLUDE. Sans lignée disponible : le ROR seul."""
    data_dir = Path(data_dir)
    base = pd.DataFrame({"ror": sorted(set(rors))})
    base["entite_id"] = base["ror"]
    if not ((data_dir / "inst_lineage.parquet").exists() and (data_dir / "institutions.parquet").exists()):
        print(f"Lignée OpenAlex absente de {data_dir} : pas de rattachement aux tutelles")
        return base
    from .lineage_check import apply_lineage_cuts
    excl = pays_list_sql([r.split("/")[-1] for r in (etab_exclude or [])]) or "NULL"
    with duckdb.connect() as con:
        apply_lineage_cuts(con, data_dir, lineage_cuts, table="lin_ok")
        con.register("base", base[["ror"]])
        anc = con.execute(f"""
            SELECT DISTINCT i.ror, l.anc_ror AS entite_id
            FROM lin_ok l
            JOIN read_parquet('{data_dir}/institutions.parquet') i ON i.inst_id = l.inst_id
            WHERE i.ror IN (SELECT ror FROM base)
              AND l.anc_ror IS NOT NULL AND l.anc_ror <> '' AND l.anc_ror NOT IN ({excl})
        """).df()
    return pd.concat([base, anc], ignore_index=True).drop_duplicates()


# --- Calcul ----------------------------------------------------------------------------

def _methode(meta: dict) -> list:
    return [
        "Source : open data de l'enseignement supérieur et de la recherche, jeu « "
        f"{meta['source']['dataset']} » (candidatures évaluées et projets financés par l'ERC).",
        "Candidature : un projet déposé à un appel ERC. Lauréat : un projet financé. Taux de succès : "
        "projets financés / candidatures, pour la même entité, la même période et les mêmes panels.",
        "Discipline : panel ERC du projet (mêmes codes que les disciplines de la bibliométrie). "
        "Les Proof of Concept n'ont pas de panel : ils ne comptent que pour « toutes disciplines ».",
        "Établissement : structure d'accueil au moment de la candidature, identifiée par son ROR "
        "(directement ou via Paysage)"
        + (" ; une candidature compte aussi pour les tutelles de cette structure, comme les articles"
           if meta["parametres"]["rattachement_lignee"] else "") + ".",
        "Pays : pays de la structure d'accueil, pas la nationalité du porteur.",
        "Comptage entier : un projet compte pour chaque établissement et chaque pays d'accueil "
        "(Synergy). Les effectifs sont additionnés avant le calcul des taux et des parts.",
    ]


def compute_erc(dataset: str = ERC_ODS_DATASET, erc_dir: str = ERC_DIR, data_dir: str = DATA_DIR,
                ref_dir: str = REFERENTIEL_DIR, classement: str = CLASSEMENT,
                stage_laureat: str = ERC_STAGE_LAUREAT, types_bourse=ERC_TYPES_BOURSE,
                rattachement_lignee: bool = ERC_RATTACHEMENT_LIGNEE,
                pays_comparaison=ERC_PAYS_COMPARAISON, pays_comparaison_n: int = ERC_PAYS_COMPARAISON_N,
                telecharger: bool = True, verbose: bool = True) -> dict:
    """Calcule les effectifs ERC et écrit les sorties dans erc_dir (voir l'en-tête).
    Renvoie un résumé (nombres de projets, taux de rattachement, fichiers)."""
    out = Path(erc_dir)
    out.mkdir(parents=True, exist_ok=True)
    brut = lire_erc(dataset, erc_dir, telecharger)
    stages = brut["stage"].str.strip().str.lower().value_counts().to_dict()
    roles = brut["role"].str.strip().str.lower().value_counts().to_dict() if "role" in brut else {}
    projets, accueil = preparer_candidatures(brut, stage_laureat, types_bourse)
    accueil = rattacher_ror(accueil, ref_dir)

    # Structures sans ROR : à corriger dans Paysage
    nr = accueil[accueil["ror"].isna()]
    non_rattaches = (nr.groupby(["entities_id", "type_identifiant"], dropna=False)
                     .agg(nom=("entities_name", _plus_frequent), pays_code=("pays_code", _plus_frequent),
                          nb_candidatures=("project_id", "nunique"))
                     .reset_index().sort_values("nb_candidatures", ascending=False))
    non_rattaches.to_csv(out / "erc_non_rattaches.csv", sep=";", index=False, encoding="utf-8")

    # Établissements : ROR d'accueil, puis tutelles
    acc_ror = accueil.dropna(subset=["ror"])[["project_id", "ror"]].drop_duplicates()
    if rattachement_lignee:
        lien = tutelles_ror(acc_ror["ror"], data_dir)
    else:
        lien = pd.DataFrame({"ror": acc_ror["ror"].unique(), "entite_id": acc_ror["ror"].unique()})
    cand_etab = acc_ror.merge(lien, on="ror")[["project_id", "entite_id"]].drop_duplicates()
    cand_pays = (accueil.dropna(subset=["pays_code"])
                 .assign(entite_id=lambda x: x["pays_code"].where(~x["pays_code"].isin(PAYS_FR), "FR"))
                 [["project_id", "entite_id"]].drop_duplicates())

    con = duckdb.connect()
    con.register("projets", projets)
    con.register("cand_etab", cand_etab)
    con.register("cand_pays", cand_pays)
    agreg = """count(*)::BIGINT AS nb_candidatures,
               count(*) FILTER (WHERE p.laureat)::BIGINT AS nb_candidatures_retenues"""
    eff = con.execute(f"""
        SELECT 'monde' AS entite_type, '{MONDE_ID}' AS entite_id, p.annee_appel, p.discipline_code,
               p.type_bourse, {agreg}
        FROM projets p GROUP BY ALL
        UNION ALL
        SELECT 'pays', c.entite_id, p.annee_appel, p.discipline_code, p.type_bourse, {agreg}
        FROM cand_pays c JOIN projets p USING (project_id) GROUP BY ALL
        UNION ALL
        SELECT 'etablissement', c.entite_id, p.annee_appel, p.discipline_code, p.type_bourse, {agreg}
        FROM cand_etab c JOIN projets p USING (project_id) GROUP BY ALL
        ORDER BY 1, 2, 3, 4, 5
    """).df()
    con.execute(f"COPY (SELECT * FROM eff) TO '{out / 'erc_effectifs.parquet'}' (FORMAT parquet)")

    # Entités présentes (complètent la collection « entites »)
    tot = (eff[eff["entite_type"] != "monde"].groupby(["entite_type", "entite_id"])
           [["nb_candidatures", "nb_candidatures_retenues"]].sum().reset_index())
    # (les sommes sur années et panels sont exactes : un projet n'a qu'une année et un panel)
    corresp = charger_correspondance_paysage(ref_dir)
    noms_ref = corresp.drop_duplicates("ror").set_index("ror")["nom"].to_dict()
    infos_acc = acc_ror.merge(accueil[["project_id", "ror", "entities_name", "pays_code"]],
                              on=["project_id", "ror"])
    noms_erc = infos_acc.groupby("ror")["entities_name"].agg(_plus_frequent).to_dict()
    pays_erc = infos_acc.groupby("ror")["pays_code"].agg(_plus_frequent).to_dict()
    # tutelle sans ligne d'accueil directe : pays le plus fréquent de ses structures
    pays_tut = (acc_ror.merge(lien, on="ror").assign(pays=lambda x: x["ror"].map(pays_erc))
                .groupby("entite_id")["pays"].agg(_plus_frequent).to_dict())
    etab = tot["entite_type"] == "etablissement"
    tot["nom"] = [noms_ref.get(i) or noms_erc.get(i) or i if e else PAYS_NOMS.get(i, i)
                  for i, e in zip(tot["entite_id"], etab)]
    tot["pays_code"] = [(pays_erc.get(i) or pays_tut.get(i)) if e else i
                        for i, e in zip(tot["entite_id"], etab)]
    tot = tot.sort_values(["entite_type", "nb_candidatures"], ascending=[False, False])
    con.register("tot", tot)
    con.execute(f"COPY (SELECT entite_id, entite_type, nom, pays_code, nb_candidatures, "
                f"nb_candidatures_retenues FROM tot) TO '{out / 'erc_entites.parquet'}' (FORMAT parquet)")
    con.close()

    # Pays comparés : liste imposée, ou les N pays qui déposent le plus (France comprise)
    pays_tot = tot[tot["entite_type"] == "pays"].set_index("entite_id")["nb_candidatures"]
    if pays_comparaison:
        compares = list(dict.fromkeys(["FR", *pays_comparaison]))
    else:
        compares = list(dict.fromkeys(["FR", *pays_tot.sort_values(ascending=False)
                                       .head(pays_comparaison_n).index]))

    # Panels inconnus de la bibliométrie (filtre discipline commun)
    f_disc = Path(data_dir) / "disciplines" / f"{classement}.parquet"
    panels = sorted(set(projets["discipline_code"].dropna()))
    hors_biblio = []
    if f_disc.exists():
        connus = {r[0] for r in duckdb.sql(f"SELECT DISTINCT discipline_code "
                                           f"FROM read_parquet('{f_disc}')").fetchall()}
        hors_biblio = [p for p in panels if p not in connus]

    nb_acc = accueil.drop_duplicates("project_id").shape[0]
    proj_rattaches = acc_ror["project_id"].nunique()
    annees = sorted(int(a) for a in projets["annee_appel"].dropna().unique())
    meta = {
        "source": {"dataset": dataset, "date_extraction": dt.date.today().isoformat()},
        "date_calcul": dt.date.today().isoformat(),
        "annees": annees,
        "types_bourse": sorted(projets["type_bourse"].dropna().unique().tolist()),
        "panels": panels,
        "pays_compares": compares,
        "parametres": {"stage_laureat": stage_laureat, "types_bourse": list(types_bourse or []),
                       "rattachement_lignee": bool(rattachement_lignee),
                       "pays_comparaison": list(pays_comparaison or []),
                       "pays_comparaison_n": pays_comparaison_n},
        "controles": {
            "nb_lignes": int(len(brut)), "valeurs_stage": stages, "valeurs_role": roles,
            "nb_candidatures": int(len(projets)), "nb_laureats": int(projets["laureat"].sum()),
            "nb_candidatures_rattachees_etablissement": int(proj_rattaches),
            "part_rattachees_etablissement": round(proj_rattaches / nb_acc, 4) if nb_acc else None,
            "identifiants": accueil["type_identifiant"].value_counts().to_dict(),
            "nb_structures_non_rattachees": int(len(non_rattaches)),
            "panels_hors_bibliometrie": hors_biblio,
        },
    }
    meta["methode"] = _methode(meta)
    meta["sources"] = (f"Source : open data ESR, jeu {dataset} (extrait le "
                       f"{meta['source']['date_extraction']}) ; correspondance Paysage -> ROR.")
    (out / "erc_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")

    if verbose:
        c = meta["controles"]
        print(f"ERC : {c['nb_lignes']:,} lignes, {c['nb_candidatures']:,} candidatures, "
              f"{c['nb_laureats']:,} lauréats ({c['nb_laureats'] / max(c['nb_candidatures'], 1):.1%})")
        print(f"  stage : {stages}")
        print(f"  identifiants d'accueil : {c['identifiants']}")
        print(f"  candidatures rattachées à un établissement (ROR) : {proj_rattaches:,} / {nb_acc:,} "
              f"({c['part_rattachees_etablissement'] or 0:.1%})")
        fr_nr = non_rattaches[non_rattaches["pays_code"].isin(PAYS_FR)]
        if len(fr_nr):
            print(f"  structures françaises sans ROR : {len(fr_nr)} ({int(fr_nr['nb_candidatures'].sum())} "
                  f"candidatures), ex. " + " ; ".join(fr_nr["nom"].head(5).astype(str)))
        if hors_biblio:
            print(f"  panels absents des disciplines de la bibliométrie : {hors_biblio}")
        fr = eff[eff["entite_id"] == "FR"].groupby("annee_appel")[
            ["nb_candidatures", "nb_candidatures_retenues"]].sum()
        fr["taux_succes"] = (fr["nb_candidatures_retenues"] / fr["nb_candidatures"]).round(3)
        print("  France par année d'appel :\n" + fr.to_string())
        print(f"  pays comparés : {', '.join(compares)}")
        print(f"  -> {out}")
    return {"dossier": str(out), "nb_candidatures": meta["controles"]["nb_candidatures"],
            "nb_laureats": meta["controles"]["nb_laureats"],
            "part_rattachees_etablissement": meta["controles"]["part_rattachees_etablissement"],
            "nb_lignes_effectifs": len(eff)}
