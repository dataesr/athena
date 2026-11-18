"""
horizon_topics.py
=================
Topic OpenAlex et panel ERC des projets Horizon Europe HORS ERC (piliers 1 à 4 : MSCA,
clusters, EIC...), candidatures ET lauréats, pour les indicateurs européens. Les ERC ne sont
pas classés ici : leur panel vient de la source (panel_code, erc.py).

Méthode
-------
  Un topic par projet (project_id), calculé de la même façon pour tous les projets, financés ou
  non, pour que candidatures et lauréats d'un même panel soient comparables (taux de succès) :
    texte : title_project + abstract de la proposition (jeu des soumis), à défaut ceux du jeu des
            lauréats (projet sans ligne dans les soumis) ;
    topic : aboutness OpenAlex (aboutness.py : 0,01 $ par texte, 1 req/s, cache, plafond par
            lancement ABOUTNESS_BUDGET_USD ; relancer pour la suite) ;
    panel : topic -> panel par la table de l'étape 3 (disciplines.ajouter_disciplines), comme les
            articles.
  Les topics présents dans d'autres sources (scanR, awards OpenAlex) ne sont pas utilisés.
  Les projets ERC (thema_code ou action_id = HORIZON_TOPICS_EXCLURE) sont exclus.

Sortie : <HORIZON_DIR>/horizon_topics_<nomenclature>.parquet, une ligne par projet :
  project_id, thema_code, pilier_code, programme_code, annee_appel, soumis, laureat,
  texte_source (soumis | laureats), topic_id, topic_score, aboutness_note, discipline_code,
  discipline_nom, domaine_code, doute

Exemple :
    from project.server.main.horizon_topics import compute_horizon_topics, estimer_horizon_topics
    estimer_horizon_topics()          # nombre de textes à envoyer, coût, durée (sans appel)
    compute_horizon_topics()          # classe dans la limite du budget ; relancer pour la suite
"""

from pathlib import Path

import duckdb
import pandas as pd

from .aboutness import classer_textes, estimer
from .common import plus_frequent
from .config import (DATA_DIR, HORIZON_DIR, HORIZON_TOPICS_EXCLURE, NOMENCLATURE_DISCIPLINAIRE)
from .disciplines import ajouter_disciplines
from .horizon import lire_horizon


def topics_path(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, horizon_dir: str = HORIZON_DIR) -> Path:
    return Path(horizon_dir) / f"horizon_topics_{nomenclature}.parquet"


def _premier(s: pd.Series):
    return plus_frequent(s, ignorer_vides=True)


def projets_horizon(horizon_dir: str = HORIZON_DIR, telecharger: bool = True,
                    exclure=HORIZON_TOPICS_EXCLURE) -> pd.DataFrame:
    """Une ligne par projet hors ERC, avec le texte à classer (voir l'en-tête)."""
    exclure = {e.upper() for e in exclure}
    jeux = {}
    for jeu in ("soumis", "laureats"):
        df = lire_horizon(jeu, horizon_dir, telecharger)
        if "abstract" not in df.columns or "title_project" not in df.columns:
            raise ValueError(f"jeu {jeu} sans title_project / abstract : retirer « abstract » de "
                             f"HORIZON_COLONNES_EXCLUES et relancer avec telecharger=True")
        df = df[df["project_id"].astype(str).str.strip() != ""]
        col = lambda c: df[c].astype(str).str.strip().str.upper() if c in df.columns else pd.Series("", index=df.index)
        erc = col("thema_code").isin(exclure) | col("action_id").isin(exclure)
        df = df[~erc]
        jeux[jeu] = (df.groupby("project_id")
                     .agg(titre=("title_project", _premier), resume=("abstract", _premier),
                          thema_code=("thema_code", _premier), pilier_code=("pilier_code", _premier),
                          programme_code=("programme_code", _premier), annee_appel=("call_year", _premier))
                     .reset_index())
    s, l = jeux["soumis"], jeux["laureats"]
    p = s.merge(l, on="project_id", how="outer", suffixes=("", "_l"), indicator=True)
    p["soumis"] = p["_merge"] != "right_only"
    p["laureat"] = p["_merge"] != "left_only"
    # texte de la proposition (soumis) ; à défaut, celui du jeu des lauréats
    avec_texte_s = p["soumis"] & p["titre"].notna()
    p["texte_source"] = ["soumis" if a else "laureats" for a in avec_texte_s]
    for c in ("titre", "resume", "thema_code", "pilier_code", "programme_code", "annee_appel"):
        p[c] = p[c].where(avec_texte_s if c in ("titre", "resume") else p[c].notna(), p[f"{c}_l"])
    p["annee_appel"] = pd.to_numeric(p["annee_appel"], errors="coerce").astype("Int64")
    return p[["project_id", "thema_code", "pilier_code", "programme_code", "annee_appel", "soumis",
              "laureat", "texte_source", "titre", "resume"]]


def estimer_horizon_topics(horizon_dir: str = HORIZON_DIR, telecharger: bool = True) -> dict:
    """Sans appel à aboutness : projets, textes à envoyer, coût et durée."""
    p = projets_horizon(horizon_dir, telecharger)
    e = estimer(p.rename(columns={"project_id": "cle"})[["cle", "titre", "resume"]])
    return {"projets": len(p), "soumis": int(p["soumis"].sum()), "laureats": int(p["laureat"].sum()), **e}


def compute_horizon_topics(horizon_dir: str = HORIZON_DIR, data_dir: str = DATA_DIR,
                           nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, telecharger: bool = True,
                           verbose: bool = True) -> dict:
    """Classe les projets Horizon hors ERC (voir l'en-tête) et écrit la sortie. Les projets que
    le budget du lancement n'a pas permis de classer gardent topic_id NULL (aboutness_note) :
    relancer pour la suite (le cache évite de repayer les autres). Renvoie un résumé."""
    p = projets_horizon(horizon_dir, telecharger)
    r = classer_textes(p.rename(columns={"project_id": "cle"})[["cle", "titre", "resume"]], verbose=verbose)
    p = p.merge(r.rename(columns={"cle": "project_id"}), on="project_id", how="left")
    p = ajouter_disciplines(p, nomenclature, data_dir)
    f = topics_path(nomenclature, horizon_dir)
    f.parent.mkdir(parents=True, exist_ok=True)
    cols = ["project_id", "thema_code", "pilier_code", "programme_code", "annee_appel", "soumis", "laureat",
            "texte_source", "topic_id", "topic_score", "aboutness_note", "discipline_code", "discipline_nom",
            "domaine_code", "doute"]
    duckdb.from_df(p[cols]).write_parquet(str(f))
    non = p["topic_id"].isna()
    res = {
        "fichier": str(f), "nb_projets": int(len(p)), "nb_soumis": int(p["soumis"].sum()),
        "nb_laureats": int(p["laureat"].sum()),
        "nb_classes": int(p["discipline_code"].notna().sum()),
        "nb_sans_topic": int(non.sum()),
        "sans_topic_motifs": p.loc[non, "aboutness_note"].fillna("?").str.split(" :").str[0].value_counts().to_dict(),
        "depense_usd": r.attrs.get("depense_usd"), "arret": r.attrs.get("arret"),
        "complet": bool(not r.attrs.get("arret")),
    }
    if verbose:
        print(f"Horizon hors ERC : {res['nb_projets']:,} projets ({res['nb_soumis']:,} soumis, "
              f"{res['nb_laureats']:,} lauréats), {res['nb_classes']:,} classés dans un panel ; "
              f"sans topic : {res['sans_topic_motifs']}" + ("" if res["complet"] else " — RELANCER pour la suite"))
    return res
