"""
pipeline.py
===========
FACULTATIF : raccourci pour enchaîner les étapes 2 à 7 sur le snapshot choisi
dans config.py (SNAPSHOT). Les modules de chaque étape restent utilisables
directement ; ce fichier ne fait que les appeler dans le bon ordre.
Un nouveau snapshot s'extrait avec extract_works.extract_snapshot().

Les étapes (chacune dépend des précédentes)
-------------------------------------------
  "referentiel"           build_referentiel             référentiel Paysage / scanR (sélecteur, noms)
  "sources_institutions"  extract_sources_institutions  revues (ISSN), institutions (ROR, lignée)
  "disciplines"           build_disciplines             topic -> discipline
  "top_journals"          compute_top_journals          revues de tête + appariement SJR
  "top10"                 compute_top10 (par périmètre) top 10 % des plus cités
  "indicators"            compute_indicators            effectifs par entité/année/discipline
  "copublications"        compute_copublications        partenaires de chaque entité
  "publish"               publish                       fichiers pour tableaux (publication/)
  (publish_mongo() n'est jamais lancé automatiquement : chargement en base à la main)

  "top_journals" passe AVANT "top10" : le périmètre "revues_sjr" a besoin de
  revues_tete/sources_sjr.parquet.

Que relancer quand on change quelque chose ?
--------------------------------------------
  nouveau snapshot OpenAlex           -> extract_snapshot(), SNAPSHOT dans config.py,
                                         puis run(start="disciplines")
  fichier topic -> discipline         -> run(start="disciplines")
  fichier SJR, règles revues de tête  -> run(start="top_journals")
  liste de revues corrigée à la main  -> run(steps=["review", "indicators", "copublications", "publish"],
                                             reviewed_csv=...)
  fenêtre de citation, TOP_SHARE      -> run(start="top10")
  périmètre (types, années)           -> run(start="top_journals")
  pays, établissements, seuils étab.  -> run(start="indicators")
  fichier de corrections, alias       -> run(steps=["publish"])
  mise à jour de Paysage / scanR      -> run(steps=["referentiel", "publish"])

Chaque étape est aussi disponible seule : step_disciplines(), step_top10(perimetre="revues_sjr"), ...
Les fonctions d'origine des modules restent utilisables directement.

Exemples :
    from project.server.main import pipeline as p
    p.run()                                            # toute la chaîne
    p.run(start="disciplines")                         # de l'étape 3 à la fin
    p.run(steps=["top10", "indicators"])               # deux étapes
    p.step_top10(perimetre="revues_sjr")               # une seule étape, un seul périmètre
    p.run(steps=["review", "indicators"],
          reviewed_csv="/data/snapshots/2026-10-15/revues_tete/erc_relecture_validee.csv")

Relecture des revues de tête : compute_top_journals RÉÉCRIT <classement>_relecture.csv.
Enregistrez votre CSV corrigé sous un autre nom (ex. erc_relecture_validee.csv) et
passez-le en `reviewed_csv` : il est réappliqué juste après "top_journals".
"""

import shutil
import time
from pathlib import Path

from .config import CLASSEMENT, DATA_DIR, PERIMETRES, TOPICS_PATH
from .copublications import compute_copublications
from .disciplines import build_disciplines
from .extract_sources_institutions import extract_sources_institutions
from .indicators import compute_indicators
from .publish import publish
from .referentiel import build_referentiel
from .top10_citations import compute_top10
from .top_journals import compute_top_journals, top_journals_from_review

STEPS = ["referentiel", "sources_institutions", "disciplines", "top_journals", "top10", "indicators",
         "copublications", "publish"]


# --- Étapes individuelles ------------------------------------------------------

def step_referentiel(**kw):
    """Étape 0 : référentiel des établissements (Paysage ou scanR)."""
    return build_referentiel(**kw)


def step_sources_institutions(data_dir: str = DATA_DIR, **kw):
    """Étape 2 : revues (ISSN) et institutions (ROR, lignée)."""
    return extract_sources_institutions(data_dir=data_dir, **kw)


def step_disciplines(classement: str = CLASSEMENT, mapping_path: str = TOPICS_PATH,
                     data_dir: str = DATA_DIR, **kw):
    """Étape 3 : classement topic -> discipline."""
    return build_disciplines(mapping_path, classement=classement, data_dir=data_dir, **kw)


def step_top_journals(classement: str = CLASSEMENT, data_dir: str = DATA_DIR,
                      reviewed_csv: str = None, **kw):
    """Étape 4 : revues de tête ; réapplique `reviewed_csv` s'il est fourni."""
    res = compute_top_journals(classement, data_dir=data_dir, **kw)
    if reviewed_csv:
        step_review(reviewed_csv, classement=classement, data_dir=data_dir)
    return res


def step_review(reviewed_csv: str, classement: str = CLASSEMENT,
                data_dir: str = DATA_DIR):
    """Applique une liste de revues de tête relue à la main (sans recalcul)."""
    return top_journals_from_review(reviewed_csv, out_name=classement, data_dir=data_dir)


def step_top10(classement: str = CLASSEMENT, perimetres=PERIMETRES,
               perimetre: str = None, data_dir: str = DATA_DIR, **kw):
    """Étape 4 : top 10 % ; un périmètre (`perimetre`) ou tous ceux de `perimetres`."""
    return {p: compute_top10(classement, data_dir=data_dir, perimetre=p, **kw)
            for p in ([perimetre] if perimetre else perimetres)}


def step_indicators(classement: str = CLASSEMENT, perimetres=PERIMETRES,
                    data_dir: str = DATA_DIR, **kw):
    """Étape 6 : effectifs."""
    return compute_indicators(classement, perimetres=perimetres, data_dir=data_dir, **kw)


def step_copublications(classement: str = CLASSEMENT, perimetres=PERIMETRES,
                        data_dir: str = DATA_DIR, **kw):
    """Étape 6b : copublications."""
    return compute_copublications(classement, perimetres=perimetres, data_dir=data_dir, **kw)


def step_publish(classement: str = CLASSEMENT, data_dir: str = DATA_DIR, **kw):
    """Étape 7 : fichiers de publication (le chargement Mongo reste manuel : publish_mongo())."""
    return publish(classement, data_dir=data_dir, **kw)


def archive_indicators(data_dir: str = DATA_DIR) -> str:
    """Copie <data_dir>/indicateurs vers indicateurs_<date> (comparaison avant/après)."""
    src = Path(data_dir) / "indicateurs"
    if not src.exists():
        return None
    dst = Path(data_dir) / f"indicateurs_{time.strftime('%Y%m%d_%H%M')}"
    shutil.copytree(src, dst)
    return str(dst)


# --- Enchaînement ------------------------------------------------------------------

def run(steps=None, start: str = None, end: str = None,
        classement: str = CLASSEMENT,
        mapping_path: str = TOPICS_PATH,
        perimetres=PERIMETRES,
        reviewed_csv: str = None,
        data_dir: str = DATA_DIR,
        archive: bool = True) -> dict:
    """Lance des étapes dans l'ordre de STEPS.

    steps : liste explicite (ex. ["top10", "indicators"]) ; "review" est
            accepté pour réappliquer seulement la liste relue
    start / end : sinon, plage d'étapes (incluses) ; par défaut, toutes
    archive : copie les effectifs existants avant de les recalculer
    Renvoie les résultats de chaque étape lancée.
    """
    if steps is None:
        i0 = STEPS.index(start) if start else 0
        i1 = STEPS.index(end) if end else len(STEPS) - 1
        steps = STEPS[i0:i1 + 1]
    inconnues = set(steps) - set(STEPS) - {"review"}
    if inconnues:
        raise ValueError(f"étapes inconnues : {sorted(inconnues)} ; disponibles : {STEPS + ['review']}")
    ordre = STEPS[:4] + ["review"] + STEPS[4:]
    steps = sorted(set(steps), key=ordre.index)

    actions = {
        "referentiel": lambda: step_referentiel(),
        "sources_institutions": lambda: step_sources_institutions(data_dir),
        "disciplines": lambda: step_disciplines(classement, mapping_path, data_dir),
        "top_journals": lambda: step_top_journals(classement, data_dir, reviewed_csv),
        "review": lambda: step_review(reviewed_csv, classement, data_dir),
        "top10": lambda: step_top10(classement, perimetres, data_dir=data_dir),
        "indicators": lambda: step_indicators(classement, perimetres, data_dir),
        "copublications": lambda: step_copublications(classement, perimetres, data_dir),
        "publish": lambda: step_publish(classement, data_dir),
    }
    if "review" in steps and not reviewed_csv:
        raise ValueError("l'étape 'review' demande reviewed_csv=...")

    res, t0 = {}, time.time()
    if archive and "indicators" in steps:
        res["archive"] = archive_indicators(data_dir)
        if res["archive"]:
            print(f"Anciens effectifs archivés dans {res['archive']}")
    for s in steps:
        print(f"\n===== {s} =====")
        t = time.time()
        res[s] = actions[s]()
        print(f"----- {s} : {(time.time() - t) / 60:.1f} min")
    print(f"\nTerminé en {(time.time() - t0) / 60:.1f} min ({', '.join(steps)})")
    return res


def run_all(**kw) -> dict:
    """Toute la chaîne (équivaut à run())."""
    return run(**kw)
