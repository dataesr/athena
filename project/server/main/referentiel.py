"""
referentiel.py
==============
Étape 0 : référentiel des établissements français, tiré de Paysage (par défaut)
ou de scanR, joint aux établissements OpenAlex par le ROR.

À quoi il sert
--------------
  - le PÉRIMÈTRE du sélecteur du dashboard (quels établissements proposer) ;
  - les NOMS affichés (nom d'usage Paysage plutôt que le nom OpenAlex), les
    sigles et noms alternatifs (recherche « CNRS », « UPC »...), la catégorie
    (université, organisme de recherche...) et la ville.
  Il ne change AUCUN calcul : les articles et la lignée viennent d'OpenAlex.

Sources (config.REFERENTIEL_SOURCE)
-----------------------------------
  "paysage" : https://scanr-data.s3.gra.io.cloud.ovh.net/production/paysage_dump.jsonl.gz
              (une structure Paysage par ligne). Champs utilisés : id, displayName,
              currentName (usualName, officialName, shortName, acronymFr, acronymEn),
              nameEn, category.usualNameFr, categories.usualNameFr, status, identifiers (type, value, active),
              relations (relationTag, resourceId), currentLocalisation (city, iso3).
  "scanr"   : https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations-v2.jsonl.gz
              (référentiel des organisations de scanR). Champs utilisés : id, label,
              acronym, externalIds (type, id), isFrench, status, categories, address.

Périmètre (même règle que l'ancien paysage.get_perimeter)
---------------------------------------------------------
  Paysage : structure de tête (pas une structure interne d'une autre), active,
  avec un nom, un ROR actif, et une catégorie de REFERENTIEL_CATEGORIES.
  scanR   : organisation française, active, avec un ROR et une catégorie de
  REFERENTIEL_CATEGORIES (scanR n'indique pas les structures internes : le
  périmètre est donc un peu plus large).

Sortie
------
  <DATA_ROOT>/referentiel/<source>.parquet, une ligne par structure qui a un ROR :
    ror, referentiel_source, referentiel_id, nom, nom_officiel, nom_en, sigle,
    categorie, ville, pays_iso3, actif, structure_de_tete, dans_perimetre, categories
    (toutes les catégories : typologie, habilitations... ; sert à groupe_etablissement()),
    date_extraction
  <DATA_ROOT>/referentiel/paysage_ror.parquet (source Paysage seulement) : TOUTES les
    correspondances identifiant Paysage -> ROR, sans dédoublonnage par ROR, françaises
    et étrangères : referentiel_id, ror, nom, pays_iso3. Sert aux jeux de données qui
    désignent les structures par leur identifiant Paysage (erc.py).
  <DATA_ROOT>/referentiel/paysage_successeurs.parquet (source Paysage) : paysage_id,
    successeur_id (successeur direct : relation « structure-predecesseur » de Paysage,
    même règle que scanR). successeur_final() remonte la chaîne jusqu'à la structure
    actuelle (ex. une université fusionnée -> la nouvelle université).
  <DATA_ROOT>/referentiel/rnsr_tutelles.parquet (scanR, build_tutelles_rnsr) : rnsr,
    tutelle_id (identifiant Paysage), tutelle_nom : tutelles (« établissement
    tutelle ») de chaque structure de recherche du RNSR, comme scanR.

Exemple :
    from project.server.main.referentiel import build_referentiel
    res = build_referentiel()                 # Paysage
    res = build_referentiel(source="scanr")   # scanR
    res["par_categorie"]
"""

import datetime as dt
import gzip
import io
import json
from pathlib import Path

import duckdb
import pandas as pd
import requests

from .config import (GROUPES_ETABLISSEMENTS_REGLES, PAYSAGE_DUMP_URL, REFERENTIEL_CATEGORIES,
                     REFERENTIEL_DIR, REFERENTIEL_SOURCE, SCANR_ORGANIZATIONS_URL)

COLONNES = ["ror", "referentiel_source", "referentiel_id", "nom", "nom_officiel", "nom_en", "sigle",
            "categorie", "ville", "pays_iso3", "actif", "structure_de_tete", "dans_perimetre", "categories"]


def referentiel_path(source: str = REFERENTIEL_SOURCE, ref_dir: str = REFERENTIEL_DIR) -> Path:
    return Path(ref_dir) / f"{source}.parquet"


def correspondance_paysage_path(ref_dir: str = REFERENTIEL_DIR) -> Path:
    return Path(ref_dir) / "paysage_ror.parquet"


def successeurs_paysage_path(ref_dir: str = REFERENTIEL_DIR) -> Path:
    return Path(ref_dir) / "paysage_successeurs.parquet"


def tutelles_rnsr_path(ref_dir: str = REFERENTIEL_DIR) -> Path:
    return Path(ref_dir) / "rnsr_tutelles.parquet"


def _lignes_jsonl_gz(url_ou_fichier: str):
    """Lit un .jsonl.gz ligne par ligne (URL ou fichier local), sans tout charger
    en mémoire : organizations-v2 contient aussi des statistiques et pèse lourd."""
    if str(url_ou_fichier).startswith("http"):
        with requests.get(url_ou_fichier, stream=True, timeout=600) as r:
            r.raise_for_status()
            r.raw.decode_content = False
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for ligne in io.TextIOWrapper(gz, encoding="utf-8"):
                    if ligne.strip():
                        yield json.loads(ligne)
    else:
        ouvrir = gzip.open if str(url_ou_fichier).endswith(".gz") else open
        with ouvrir(url_ou_fichier, "rt", encoding="utf-8") as f:
            for ligne in f:
                if ligne.strip():
                    yield json.loads(ligne)


def _texte(v):
    return v.strip() if isinstance(v, str) and v.strip() else None


# --- Paysage -----------------------------------------------------------------------

def est_structure_de_tete(e: dict) -> bool:
    """Vrai si la structure n'est la structure interne d'aucune autre : une relation
    « structure-interne » dont la ressource est une AUTRE structure signifie que e
    est rattachée à cette structure (ex. une composante d'université)."""
    for r in e.get("relations") or []:
        if r.get("relationTag") == "structure-interne" and r.get("resourceId") != e.get("id"):
            return False
    return True


def ror_paysage(e: dict, actif_seulement: bool = True):
    """Premier ROR ACTIF de la structure (None sinon). L'ancienne version
    renvoyait le premier ROR trouvé même s'il était inactif.
    actif_seulement=False : à défaut de ROR actif, le premier ROR trouvé."""
    rors = [(bool(k.get("active")), k["value"].strip().rstrip("/").split("/")[-1])
            for k in e.get("identifiers") or []
            if k.get("type") == "ror" and _texte(k.get("value"))]
    actifs = [r for a, r in rors if a]
    if actifs:
        return actifs[0]
    return None if actif_seulement or not rors else rors[0][1]


def _correspondance_paysage(e: dict):
    """(referentiel_id, ror, nom, pays_iso3) si la structure a un ROR, sinon None."""
    ror = ror_paysage(e, actif_seulement=False)
    if not ror or not e.get("id"):
        return None
    cn = e.get("currentName") or {}
    nom = _texte(e.get("displayName")) or _texte(cn.get("usualName")) or _texte(cn.get("officialName"))
    return (e["id"], ror, nom, _texte((e.get("currentLocalisation") or {}).get("iso3")))


def _successeurs_paysage(e: dict) -> list:
    """[(paysage_id, successeur_id)] : relation « structure-predecesseur » dont e est
    le prédécesseur (relatedObjectId = e), la ressource étant le successeur (règle de
    scanR, paysage.build_successeur_map)."""
    return [(e["id"], r["resourceId"]) for r in e.get("relations") or []
            if r.get("relationTag") == "structure-predecesseur" and r.get("relatedObjectId") == e.get("id")
            and r.get("resourceId") and r["resourceId"] != e.get("id")]


def _ecrire(df: pd.DataFrame, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY df TO '{out}' (FORMAT parquet)")
    return out


def _ecrire_correspondance(lignes: list, ref_dir: str, successeurs: list = None) -> Path:
    df = pd.DataFrame(lignes, columns=["referentiel_id", "ror", "nom", "pays_iso3"]).drop_duplicates(
        "referentiel_id")
    out = _ecrire(df, correspondance_paysage_path(ref_dir))
    if successeurs is not None:
        _ecrire(pd.DataFrame(successeurs, columns=["paysage_id", "successeur_id"]).drop_duplicates("paysage_id"),
                successeurs_paysage_path(ref_dir))
    return out


def build_correspondance_paysage(url: str = PAYSAGE_DUMP_URL, ref_dir: str = REFERENTIEL_DIR,
                                 verbose: bool = True) -> str:
    """Écrit seulement <ref_dir>/paysage_ror.parquet (identifiant Paysage -> ROR) et
    paysage_successeurs.parquet. Inutile si build_referentiel() a été lancé avec la
    source Paysage, qui les écrit aussi."""
    lignes, successeurs = [], []
    for e in _lignes_jsonl_gz(url):
        c = _correspondance_paysage(e)
        if c:
            lignes.append(c)
        successeurs += _successeurs_paysage(e)
    out = _ecrire_correspondance(lignes, ref_dir, successeurs)
    if verbose:
        print(f"Paysage -> ROR : {len(lignes):,} structures avec un ROR, {len(successeurs):,} "
              f"successeurs -> {out}")
    return str(out)


def charger_correspondance_paysage(ref_dir: str = REFERENTIEL_DIR, construire: bool = True):
    """Table identifiant Paysage -> ROR (DataFrame) ; la construit si elle manque
    et construire=True, sinon renvoie None."""
    p = correspondance_paysage_path(ref_dir)
    if not p.exists():
        if not construire:
            return None
        build_correspondance_paysage(ref_dir=ref_dir)
    return duckdb.sql(f"SELECT * FROM read_parquet('{p}')").df()


def charger_successeurs_paysage(ref_dir: str = REFERENTIEL_DIR, construire: bool = True) -> dict:
    """{paysage_id: successeur direct} ; reconstruit la correspondance Paysage si la table
    manque (construire=True), sinon {}."""
    p = successeurs_paysage_path(ref_dir)
    if not p.exists():
        if not construire:
            return {}
        build_correspondance_paysage(ref_dir=ref_dir)
    df = duckdb.sql(f"SELECT * FROM read_parquet('{p}')").df()
    return dict(zip(df["paysage_id"], df["successeur_id"]))


def successeur_final(paysage_id, successeurs: dict):
    """Structure actuelle : suit les successeurs jusqu'au dernier (cycles ignorés),
    comme scanR (paysage.get_successeur)."""
    vus = set()
    while paysage_id in successeurs and paysage_id not in vus:
        vus.add(paysage_id)
        paysage_id = successeurs[paysage_id]
    return paysage_id


def build_tutelles_rnsr(url: str = SCANR_ORGANIZATIONS_URL, ref_dir: str = REFERENTIEL_DIR,
                        verbose: bool = True) -> str:
    """Écrit <ref_dir>/rnsr_tutelles.parquet : (rnsr, tutelle_id, tutelle_nom) pour chaque
    structure du RNSR de scanR (organizations-v2) et chacune de ses institutions de type
    « établissement tutelle » (identifiant Paysage), comme scanR (projects.py)."""
    lignes = []
    for e in _lignes_jsonl_gz(url):
        rnsrs = {_texte(k.get("id")) for k in e.get("externalIds") or [] if k.get("type") == "rnsr"}
        rnsrs = {r for r in rnsrs if r}
        if not rnsrs:
            continue
        for inst in e.get("institutions") or []:
            if not isinstance(inst, dict) or inst.get("relationType") != "établissement tutelle":
                continue
            t = _texte(inst.get("structure"))
            if t:
                lignes += [(r, t, _texte(inst.get("label"))) for r in rnsrs]
    df = pd.DataFrame(lignes, columns=["rnsr", "tutelle_id", "tutelle_nom"]).drop_duplicates(
        ["rnsr", "tutelle_id"])
    out = _ecrire(df, tutelles_rnsr_path(ref_dir))
    if verbose:
        print(f"RNSR -> tutelles : {df['rnsr'].nunique():,} structures, {len(df):,} liens -> {out}")
    return str(out)


def charger_tutelles_rnsr(ref_dir: str = REFERENTIEL_DIR, construire: bool = True) -> pd.DataFrame:
    """Table (rnsr, tutelle_id, tutelle_nom) ; la construit si elle manque (construire=True)."""
    p = tutelles_rnsr_path(ref_dir)
    if not p.exists():
        if not construire:
            return pd.DataFrame(columns=["rnsr", "tutelle_id", "tutelle_nom"])
        build_tutelles_rnsr(ref_dir=ref_dir)
    return duckdb.sql(f"SELECT * FROM read_parquet('{p}')").df()


def _ligne_paysage(e: dict, categories: set) -> dict:
    cn = e.get("currentName") or {}
    cat = (e.get("category") or {}).get("usualNameFr")
    loc = e.get("currentLocalisation") or {}
    actif = e.get("status") == "active"
    tete = est_structure_de_tete(e)
    nom = _texte(e.get("displayName")) or _texte(cn.get("usualName")) or _texte(cn.get("officialName"))
    return {
        "ror": ror_paysage(e), "referentiel_source": "paysage", "referentiel_id": e.get("id"),
        "nom": nom, "nom_officiel": _texte(cn.get("officialName")),
        "nom_en": _texte(e.get("nameEn")) or _texte(cn.get("nameEn")),
        "sigle": _texte(cn.get("acronymFr")) or _texte(cn.get("shortName")) or _texte(cn.get("acronymEn")),
        "categorie": cat, "ville": _texte(loc.get("city")) or _texte(loc.get("locality")),
        "pays_iso3": _texte(loc.get("iso3")), "actif": actif, "structure_de_tete": tete,
        "dans_perimetre": bool(tete and actif and nom and cat in categories),
        # toutes les catégories (typologie, habilitations...), par priorité Paysage
        "categories": [c["usualNameFr"] for c in sorted(
            (c for c in e.get("categories") or [] if isinstance(c, dict) and _texte(c.get("usualNameFr"))),
            key=lambda c: c.get("priority") if isinstance(c.get("priority"), (int, float)) else 999)],
    }


# --- scanR ----------------------------------------------------------------------------

def _ligne_scanr(e: dict, categories: set) -> dict:
    lab, acr = e.get("label") or {}, e.get("acronym") or {}
    ror = next((x.get("id") for x in e.get("externalIds") or []
                if x.get("type") == "ror" and _texte(x.get("id"))), None)
    adr = next((a for a in e.get("address") or [] if a.get("main")), (e.get("address") or [{}])[0])
    cats = [c for c in (e.get("categories") or []) if isinstance(c, str)]
    cat = next((c for c in cats if c in categories), cats[0] if cats else None)
    actif = e.get("status") == "active"
    nom = _texte(lab.get("fr")) or _texte(lab.get("default")) or _texte(lab.get("en"))
    return {
        "ror": ror.strip().rstrip("/").split("/")[-1] if ror else None,
        "referentiel_source": "scanr", "referentiel_id": e.get("id"),
        "nom": nom, "nom_officiel": _texte(lab.get("default")), "nom_en": _texte(lab.get("en")),
        "sigle": _texte(acr.get("fr")) or _texte(acr.get("default")) or _texte(acr.get("en")),
        "categorie": cat, "ville": _texte((adr or {}).get("city")),
        "pays_iso3": "FRA" if e.get("isFrench") else None, "actif": actif,
        "structure_de_tete": None,
        "dans_perimetre": bool(e.get("isFrench") and actif and nom and cat in categories),
        "categories": cats,
    }


def build_referentiel(source: str = REFERENTIEL_SOURCE, url: str = None,
                      categories=REFERENTIEL_CATEGORIES, ref_dir: str = REFERENTIEL_DIR,
                      verbose: bool = True) -> dict:
    """Télécharge le référentiel (Paysage ou scanR), garde les structures qui ont un
    ROR et écrit <ref_dir>/<source>.parquet (voir l'en-tête du module).
    url : autre adresse ou fichier local (.jsonl ou .jsonl.gz).
    Renvoie {'fichier', 'nb_structures', 'nb_avec_ror', 'nb_perimetre', 'par_categorie'}."""
    if source not in ("paysage", "scanr"):
        raise ValueError(f"source inconnue : {source!r} (attendu : 'paysage' ou 'scanr')")
    url = url or (PAYSAGE_DUMP_URL if source == "paysage" else SCANR_ORGANIZATIONS_URL)
    lire = _ligne_paysage if source == "paysage" else _ligne_scanr
    cats = set(categories or [])
    lignes, corresp, successeurs, total = [], [], [], 0
    for e in _lignes_jsonl_gz(url):
        total += 1
        ligne = lire(e, cats)
        if ligne["ror"]:
            lignes.append(ligne)
        if source == "paysage":
            c = _correspondance_paysage(e)
            if c:
                corresp.append(c)
            successeurs += _successeurs_paysage(e)
    if source == "paysage":
        _ecrire_correspondance(corresp, ref_dir, successeurs)
    df = pd.DataFrame(lignes, columns=COLONNES)
    # Un même ROR sur plusieurs structures (ancienne et nouvelle, par exemple) :
    # on garde en priorité celle du périmètre, puis une structure active.
    df = (df.sort_values(["dans_perimetre", "actif"], ascending=False)
            .drop_duplicates("ror").reset_index(drop=True))
    df["date_extraction"] = dt.date.today().isoformat()

    out = referentiel_path(source, ref_dir)
    out.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY df TO '{out}' (FORMAT parquet)")

    par_cat = (df[df["dans_perimetre"]].groupby("categorie").size()
               .sort_values(ascending=False).rename("nb_structures").reset_index())
    res = {"fichier": str(out), "nb_structures": total, "nb_avec_ror": len(df),
           "nb_perimetre": int(df["dans_perimetre"].sum()), "par_categorie": par_cat}
    if verbose:
        print(f"{source} : {total:,} structures lues, {len(df):,} avec un ROR, "
              f"{res['nb_perimetre']:,} dans le périmètre -> {out}")
        print(par_cat.to_string(index=False))
    return res


def groupe_etablissement(categorie, categories=None, regles=GROUPES_ETABLISSEMENTS_REGLES) -> str:
    """Groupe de comparaison (config.GROUPES_ETABLISSEMENTS) : première règle dont une
    catégorie figure parmi la catégorie principale et toutes les catégories ; sinon « autres »."""
    autres = [] if categories is None or isinstance(categories, (str, float)) else list(categories)
    toutes = {c for c in [categorie, *autres] if isinstance(c, str)}
    return next((code for code, cats in regles if toutes.intersection(cats)), "autres")


def charger_referentiel(source: str = REFERENTIEL_SOURCE, ref_dir: str = REFERENTIEL_DIR):
    """Le référentiel déjà construit (DataFrame), ou None s'il n'existe pas."""
    p = referentiel_path(source, ref_dir)
    if not p.exists():
        return None
    return duckdb.sql(f"SELECT * FROM read_parquet('{p}')").df()
