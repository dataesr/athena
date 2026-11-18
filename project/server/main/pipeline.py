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
  "erc"                   compute_erc                   candidatures et lauréats ERC (open data ESR)
  "prix"                  compute_prix                  lauréats des prix (IUF...) par établissement
  "indicators"            compute_indicators            effectifs par entité/année/discipline
  "publications"          extraire_publications         publications des lauréats et signatures des
                                                        viviers, en UNE lecture des works (long)
  "prix_disciplines"      compute_prix_disciplines      panel ERC des lauréats par leurs publications
  "viviers"               compute_viviers               chercheurs actifs par pays, établissement
                                                        et discipline
  "copublications"        compute_copublications        partenaires de chaque entité
  "awards"                extract_awards + compute_awards  financements OpenAlex par pays du
                                                        financeur et panel ERC (relit les awards
                                                        du snapshot ; reprise possible)
  "projets_scanr"         build_projets_scanr           projets scanR (ANR, UE...) + topic de
                                                        l'award OpenAlex + panel ERC (après "awards")
  "horizon_topics"        compute_horizon_topics        topic (aboutness) et panel ERC des projets
                                                        Horizon hors ERC, candidatures et lauréats
  "anr"                   compute_anr                   projets ANR par établissement (tous les
                                                        partenaires, tutelles), panel ERC par
                                                        aboutness -> tables anr_*
  "publish"               publish                       fichiers pour tableaux (publication/)
  (publish_mongo() n'est jamais lancé automatiquement : chargement en base à la main)

  "top_journals" passe AVANT "top10" : le périmètre "revues_sjr" a besoin de
  revues_tete/sources_sjr.parquet.
  "erc" passe AVANT "indicators" et "viviers" : ces étapes calculent aussi les articles et
  les chercheurs actifs des pays de la comparaison ERC (intensité, taux de candidature).
  "publications" passe APRÈS "prix" (lauréats cherchés) et "indicators" (pays de la
  bibliométrie lus pour les viviers). "prix_disciplines" et "viviers" relancent
  l'extraction s'il manque des fichiers (reprise : seuls les fichiers manquants sont relus).

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
  mise à jour de Paysage / scanR      -> run(steps=["referentiel", "erc", "prix", "publish"])
  mise à jour des prix                -> run(steps=["prix", "prix_disciplines", "publish"])
                                         (seuls les nouveaux lauréats déclenchent une relecture des
                                         works, pour les prix seulement)
  seuils des viviers (actifs, top 10 %) -> run(steps=["viviers", "publish"]) (pas de relecture des works)
                                         ; liste des pays (ERC ou biblio) ou VIVIER_ANNEE_DEBUT avancé
                                         -> relecture des works
  mise à jour des données ERC         -> run(steps=["erc", "publish"]) ; si erc_meta.pays_compares
                                         change : run(steps=["erc", "indicators", "copublications",
                                         "publish"]) (publish() le signale)

Chaque étape est aussi disponible seule : step_disciplines(), step_top10(perimetre="revues_sjr"), ...
Les fonctions d'origine des modules restent utilisables directement.

Exemples :
    from project.server.main import pipeline as p
    p.run()                                            # toute la chaîne
    p.run(start="disciplines")                         # de l'étape 3 à la fin
    p.run(steps=["top10", "indicators"])               # deux étapes
    p.step_top10(perimetre="revues_sjr")               # une seule étape, un seul périmètre
    p.run(steps=["review", "indicators"],
          reviewed_csv="/data/snapshots/2026-10-15/revues_tete/panel_erc_relecture_validee.csv")

Relecture des revues de tête : compute_top_journals RÉÉCRIT <nomenclature>_relecture.csv.
Enregistrez votre CSV corrigé sous un autre nom (ex. panel_erc_relecture_validee.csv) et
passez-le en `reviewed_csv` : il est réappliqué juste après "top_journals".
"""

import shutil
import time
from pathlib import Path

from .config import NOMENCLATURE_DISCIPLINAIRE, DATA_DIR, PERIMETRES, TOPICS_PATH
from .copublications import compute_copublications
from .disciplines import build_disciplines
from .erc import compute_erc
from .extract_awards import compute_awards, extract_awards
from .extract_sources_institutions import extract_sources_institutions
from .projets_scanr import build_projets_scanr
from .anr import compute_anr
from .horizon_topics import compute_horizon_topics
from .indicators import compute_indicators
from .prix import compute_prix
from .prix_disciplines import compute_prix_disciplines
from .publications_auteurs import extraire_publications
from .publish import publish
from .referentiel import build_referentiel
from .startups import compute_startups
from .top10_citations import compute_top10
from .top_journals import compute_top_journals, top_journals_from_review
from .viviers import compute_viviers

STEPS = ["referentiel", "sources_institutions", "disciplines", "top_journals", "top10", "erc", "prix",
         "indicators", "publications", "prix_disciplines", "viviers", "copublications", "awards", "projets_scanr", "horizon_topics", "anr", "startups", "publish"]


# --- Étapes individuelles ------------------------------------------------------

def step_referentiel(**kw):
    """Étape 0 : référentiel des établissements (Paysage ou scanR)."""
    return build_referentiel(**kw)


def step_sources_institutions(data_dir: str = DATA_DIR, **kw):
    """Étape 2 : revues (ISSN) et institutions (ROR, lignée)."""
    return extract_sources_institutions(data_dir=data_dir, **kw)


def step_disciplines(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, mapping_path: str = TOPICS_PATH,
                     data_dir: str = DATA_DIR, **kw):
    """Étape 3 : nomenclature topic -> discipline."""
    return build_disciplines(mapping_path, nomenclature=nomenclature, data_dir=data_dir, **kw)


def step_top_journals(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, data_dir: str = DATA_DIR,
                      reviewed_csv: str = None, **kw):
    """Étape 4 : revues de tête ; réapplique `reviewed_csv` s'il est fourni."""
    res = compute_top_journals(nomenclature, data_dir=data_dir, **kw)
    if reviewed_csv:
        step_review(reviewed_csv, nomenclature=nomenclature, data_dir=data_dir)
    return res


def step_review(reviewed_csv: str, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE,
                data_dir: str = DATA_DIR):
    """Applique une liste de revues de tête relue à la main (sans recalcul)."""
    return top_journals_from_review(reviewed_csv, out_name=nomenclature, data_dir=data_dir)


def step_top10(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, perimetres=PERIMETRES,
               perimetre: str = None, data_dir: str = DATA_DIR, **kw):
    """Étape 4 : top 10 % ; un périmètre (`perimetre`) ou tous ceux de `perimetres`."""
    return {p: compute_top10(nomenclature, data_dir=data_dir, perimetre=p, **kw)
            for p in ([perimetre] if perimetre else perimetres)}


def step_indicators(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, perimetres=PERIMETRES,
                    data_dir: str = DATA_DIR, **kw):
    """Étape 6 : effectifs."""
    return compute_indicators(nomenclature, perimetres=perimetres, data_dir=data_dir, **kw)


def step_copublications(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, perimetres=PERIMETRES,
                        data_dir: str = DATA_DIR, **kw):
    """Étape 6b : copublications."""
    return compute_copublications(nomenclature, perimetres=perimetres, data_dir=data_dir, **kw)


def step_erc(data_dir: str = DATA_DIR, **kw):
    """Bourses ERC : candidatures et lauréats par entité, année d'appel et panel."""
    return compute_erc(data_dir=data_dir, **kw)


def step_prix(data_dir: str = DATA_DIR, **kw):
    """Prix et distinctions : lauréats par établissement, année et prix."""
    return compute_prix(data_dir=data_dir, **kw)


def step_publications(data_dir: str = DATA_DIR, **kw):
    """Publications des lauréats et signatures des viviers : une seule lecture des works,
    seuls les fichiers manquants de chaque sortie sont lus."""
    res = extraire_publications(data_dir=data_dir, **kw)
    if res["erreurs"]:
        raise RuntimeError(f"{len(res['erreurs'])} fichier(s) works non lus : relancer (reprise possible)")
    return res


def step_prix_disciplines(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE):
    """Discipline des lauréats de prix (extraction complétée si besoin, prix et viviers ensemble)."""
    step_publications(data_dir)
    return compute_prix_disciplines(data_dir=data_dir, nomenclature=nomenclature)


def step_viviers(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE):
    """Viviers de chercheurs (extraction complétée si besoin, prix et viviers ensemble)."""
    step_publications(data_dir)
    return compute_viviers(data_dir=data_dir, nomenclature=nomenclature)


def step_awards(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, **kw):
    """Financements OpenAlex : extraction (seuls les fichiers manquants sont lus), puis
    pays du financeur x panel ERC."""
    res = extract_awards(**kw)          # snapshot le plus récent (AWARDS_SNAPSHOT), indépendant des works
    if res["erreurs"]:
        raise RuntimeError(f"{len(res['erreurs'])} fichier(s) awards non lus : relancer (reprise possible)")
    return compute_awards(nomenclature, date=res["snapshot"], data_dir=data_dir)


def step_projets_scanr(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, **kw):
    """Projets scanR enrichis du topic OpenAlex (awards) et du panel ERC."""
    return build_projets_scanr(nomenclature, data_dir=data_dir, **kw)


def step_horizon_topics(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, **kw):
    """Projets Horizon hors ERC (candidatures et lauréats) : topic par aboutness, panel ERC."""
    return compute_horizon_topics(data_dir=data_dir, nomenclature=nomenclature, **kw)


def step_anr(data_dir: str = DATA_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, **kw):
    """Projets ANR : affiliations (règle commune) et panel ERC -> <DATA_DIR>/anr/anr_*."""
    return compute_anr(data_dir=data_dir, nomenclature=nomenclature, **kw)


def step_startups(data_dir: str = DATA_DIR, **kw):
    """startups ESR : créations par année et ROR."""
    return compute_startups(data_dir=data_dir, **kw)


def step_publish(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, data_dir: str = DATA_DIR, **kw):
    """Étape 7 : fichiers de publication (le chargement Mongo reste manuel : publish_mongo())."""
    return publish(nomenclature, data_dir=data_dir, **kw)


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
        nomenclature: str = NOMENCLATURE_DISCIPLINAIRE,
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
        if not start and not end:
            steps = [step for step in steps if step != "startups"]
    inconnues = set(steps) - set(STEPS) - {"review"}
    if inconnues:
        raise ValueError(f"étapes inconnues : {sorted(inconnues)} ; disponibles : {STEPS + ['review']}")
    ordre = STEPS[:4] + ["review"] + STEPS[4:]
    steps = sorted(set(steps), key=ordre.index)

    actions = {
        "referentiel": lambda: step_referentiel(),
        "sources_institutions": lambda: step_sources_institutions(data_dir),
        "disciplines": lambda: step_disciplines(nomenclature, mapping_path, data_dir),
        "top_journals": lambda: step_top_journals(nomenclature, data_dir, reviewed_csv),
        "review": lambda: step_review(reviewed_csv, nomenclature, data_dir),
        "top10": lambda: step_top10(nomenclature, perimetres, data_dir=data_dir),
        "indicators": lambda: step_indicators(nomenclature, perimetres, data_dir),
        "copublications": lambda: step_copublications(nomenclature, perimetres, data_dir),
        "erc": lambda: step_erc(data_dir),
        "prix": lambda: step_prix(data_dir),
        "publications": lambda: step_publications(data_dir),
        "prix_disciplines": lambda: step_prix_disciplines(data_dir, nomenclature),
        "viviers": lambda: step_viviers(data_dir, nomenclature),
        "awards": lambda: step_awards(data_dir, nomenclature),
        "projets_scanr": lambda: step_projets_scanr(data_dir, nomenclature),
        "horizon_topics": lambda: step_horizon_topics(data_dir, nomenclature),
        "anr": lambda: step_anr(data_dir, nomenclature),
        "startups": lambda: step_startups(data_dir),
        "publish": lambda: step_publish(nomenclature, data_dir),
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
