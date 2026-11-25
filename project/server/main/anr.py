"""
anr.py
======
Projets ANR par établissement, pays, année, panel ERC et action (instrument ANR), tables
anr_* distinctes des bourses ERC (erc_*) et de la bibliométrie (biblio_*).

Source
------
  Base des projets de scanR (projects_denormalized.jsonl.gz, copie locale de projets_scanr.py),
  types ANR_TYPES : projets financés (lauréats). Les candidatures ANR ne sont pas encore
  disponibles ; elles prendront le même chemin (panel par aboutness, mêmes affiliations).
  Les affiliations sont déjà traitées en amont par scanR-backend : participant_id est un RNSR
  (laboratoire) ou un identifiant Paysage ; les partenaires non identifiés
  (participant_to_identify, souvent des entreprises) n'ont pas d'identifiant.

Règles (communes avec l'ERC et les prix)
---------------------------------------
  Établissement : affiliations.rattacher_structures sur participant_id : la structure (Paysage
    -> successeur -> ROR) et, pour un laboratoire RNSR, TOUTES ses tutelles (relation
    « établissement tutelle » du référentiel scanR, pas les « établissements participants »),
    puis, si ANR_RATTACHEMENT_LIGNEE, les tutelles OpenAlex (lignée du snapshot).
    TOUS les partenaires comptent ; le rôle de coordinateur est gardé (nb_projets_coordination)
    mais n'est pas le comptage par défaut.
  Pays : pays de la structure (mainAddress.iso3 de scanR), outre-mer sous FR.
  Panel ERC : topic du titre + résumé par aboutness (aboutness.py, avec cache) pour TOUS les
    projets, pour que lauréats et candidatures (à venir) soient classés de la même façon ; topic ->
    panel par la table de l'étape 3 (disciplines.ajouter_disciplines), comme les articles.
    topic_source = aboutness | NULL (non classé : budget du lancement atteint -> relancer, ou texte
    trop court). Les topics de scanR et des awards OpenAlex ne sont pas utilisés.
  Montants : ceux des participations de scanR, qui font foi (participation_funding par
    participation, project_budgetTotal / project_budgetFinanced par projet) ; à défaut (partenaire
    ou projet sans participation identifiée dans scanR), ceux de la base des projets (mêmes valeurs
    quand les deux existent ; alerte sinon). source_montant = scanr_participations | scanr_projets.
    montant_participations d'une entité n'est PAS additif entre entités : une participation compte
    en entier pour sa structure, chacune de ses tutelles et leur lignée.
  Comptage entier : un projet compte 1 pour chaque entité présente.

Sorties (dans ANR_DIR = <DATA_DIR>/anr/)
---------------------------------------
  anr_projets.parquet          une ligne par projet : projet_id, type, annee, action_code,
                               action_libelle, budget_total, budget_finance, source_montant,
                               topic_id, topic_source,
                               discipline_code, domaine_code, doute, nb_participations,
                               nb_participations_rattachees
  anr_participations.parquet   une ligne par participation : projet_id, participation_id, role,
                               coordinateur, participant_id, type_identifiant, nom, pays_code,
                               montant, source_montant, ror, nb_tutelles_rnsr
  anr_effectifs.parquet        entite_type (pays | etablissement) x entite_id x annee x
                               discipline_code x action_code : nb_projets,
                               nb_projets_coordination, nb_participations, montant_participations
  anr_entites.parquet          entités présentes : entite_id, entite_type, nom, pays_code, nb_projets
  anr_non_rattaches.csv        participations sans établissement (à identifier dans scanR)
  anr_controle_scanr.csv       participations dont les participants (structure + tutelles) diffèrent
                               des participations de scanR, qui font foi (controle_scanr.py ; alerte
                               au-delà de SCANR_SEUIL_ALERTE)
  anr_meta.json                source, paramètres, couverture (panel, rattachement), méthode

Exemple :
    from project.server.main.anr import compute_anr
    res = compute_anr()
"""

import datetime as dt
import json
from pathlib import Path

import duckdb
import pandas as pd

from .affiliations import lignes_entites, rattacher_structures, tutelles_ror
from .common import pays_entite
from .config import (ANR_DIR, ANR_RATTACHEMENT_LIGNEE, ANR_TYPES, DATA_DIR,
                     NOMENCLATURE_DISCIPLINAIRE, PROJETS_DIR, REFERENTIEL_DIR)
from .controle_scanr import appliquer_montants_scanr, controler_participants
from .disciplines import ajouter_disciplines
from .horizon import pays_iso2
from .projets_scanr import _iter_scanr, download_scanr
from .programmes import effectifs_projets, nommer_entites, non_rattaches


def _json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def _langue(v, ordre=("en", "fr", "default")):
    """Texte d'un champ multilingue scanR ({"fr": ..., "en": ...}), anglais d'abord (comme
    l'essentiel du corpus OpenAlex sur lequel le classifieur est entraîné)."""
    v = _json(v)
    if isinstance(v, dict):
        for k in ordre:
            if v.get(k):
                return str(v[k])
        return next((str(x) for x in v.values() if x), None)
    return str(v) if v else None


def lire_anr(f_scanr: Path, types=ANR_TYPES) -> tuple:
    """(projets, participations) des projets scanR des types demandés."""
    projets, parts = [], []
    for p in _iter_scanr(f_scanr):
        if p.get("type") not in types:
            continue
        pid = str(p.get("id"))
        action = _json(p.get("action")) or {}
        projets.append({
            "projet_id": pid, "type": p.get("type"),
            "annee": int(p["year"]) if p.get("year") else None,
            "action_code": action.get("id") if isinstance(action, dict) else None,
            "action_libelle": _langue(action.get("label")) if isinstance(action, dict) else None,
            "budget_total": p.get("budgetTotal"), "budget_finance": p.get("budgetFinanced"),
            "titre": _langue(p.get("label")), "resume": _langue(p.get("description")),
        })
        for k, q in enumerate(_json(p.get("participants")) or []):
            st = q.get("structure") or {}
            adr = st.get("mainAddress") or {}
            adr = adr[0] if isinstance(adr, list) and adr else adr
            # nom : structure scanR, sinon libellé de la participation (« Nom__-__ANR-…-01 »)
            nom = _langue(st.get("label"), ("fr", "default", "en")) \
                or str(_langue(q.get("label")) or "").split("__-__")[0] or None
            # identifiant du participant : structure.id, comme scanR (participations), à défaut
            # participant_id (SIRET, UAI... non résolus par scanR)
            ident = st.get("id") or q.get("participant_id")
            parts.append({
                "projet_id": pid, "participation_id": q.get("id") or f"{pid}-{k}",
                "role": q.get("role"), "coordinateur": q.get("role") == "coordinator",
                "entities_id": ident, "rnsr": ident, "identifie_scanr": bool(st.get("id")),
                # tutelles de la structure dans scanR (« établissement tutelle » seulement), comme
                # les participations de scanR ; None si structure non identifiée
                "tutelles": ([str(x["structure"]) for x in st.get("institutions") or []
                              if isinstance(x, dict) and x.get("relationType") == "établissement tutelle"
                              and x.get("structure")] if st.get("id") else None),
                "entities_name": nom,
                "pays_code": pays_iso2(adr.get("iso3")) if isinstance(adr, dict) else None,
                "montant": q.get("funding"),
            })
    return pd.DataFrame(projets), pd.DataFrame(parts)


def panels_projets(projets: pd.DataFrame, nomenclature: str, data_dir: str, verbose: bool) -> tuple:
    """topic_id (aboutness sur titre + résumé, pour TOUS les projets : lauréats et, plus tard,
    candidatures classés de la même façon), topic_source, discipline_code..."""
    from .aboutness import classer_textes
    r = classer_textes(projets[["projet_id", "titre", "resume"]].rename(columns={"projet_id": "cle"}),
                       verbose=verbose)
    out = projets.merge(r.rename(columns={"cle": "projet_id"}), on="projet_id", how="left")
    out["topic_source"] = out["topic_id"].notna().map({True: "aboutness", False: None})
    return ajouter_disciplines(out, nomenclature, data_dir), dict(r.attrs)


def compute_anr(anr_dir: str = ANR_DIR, data_dir: str = DATA_DIR, projets_dir: str = PROJETS_DIR,
                ref_dir: str = REFERENTIEL_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE,
                types=ANR_TYPES, rattachement_lignee: bool = ANR_RATTACHEMENT_LIGNEE,
                verbose: bool = True) -> dict:
    """Calcule les tables anr_* (voir l'en-tête). Renvoie un résumé (valeurs simples)."""
    out = Path(anr_dir)
    out.mkdir(parents=True, exist_ok=True)
    f_scanr = download_scanr(projets_dir, verbose=verbose)
    projets, parts = lire_anr(f_scanr, types)
    if verbose:
        print(f"ANR ({', '.join(types)}) : {len(projets):,} projets, {len(parts):,} participations "
              f"({parts['entities_id'].notna().mean():.1%} identifiées dans scanR)")

    # Montants : participations de scanR (font foi), à défaut base des projets (controle_scanr)
    projets, parts, controle_montants = appliquer_montants_scanr(projets, parts, types=types,
                                                                 projets_dir=projets_dir, verbose=verbose)

    # Panel ERC
    projets, info_aboutness = panels_projets(projets, nomenclature, data_dir, verbose)
    aboutness_complet = bool(info_aboutness.get("complet"))

    # Affiliations : règle commune (structure + toutes les tutelles RNSR + lignée OpenAlex)
    parts, liens = rattacher_structures(parts, ref_dir)
    # Contrôle : mêmes participants (structure + tutelles) que les participations de scanR, qui
    # font foi ; seules les structures identifiées par scanR sont comparées (scanR n'a pas de
    # participation pour les autres, résolues ici par SIRET / UAI)
    controle = controler_participants(parts[parts["identifie_scanr"]], types=types, projets_dir=projets_dir,
                                      out_csv=str(out / "anr_controle_scanr.csv"), verbose=verbose)
    rors = sorted(set(liens["ror"]))
    lien = (tutelles_ror(rors, data_dir) if rattachement_lignee
            else pd.DataFrame({"ror": rors, "entite_id": rors}))
    cols = ["projet_id", "participation_id", "coordinateur", "montant"]
    lignes = lignes_entites(parts, liens, lien, cols, monde=False)

    # Effectifs : calcul commun aux programmes (programmes.effectifs_projets), lauréats seulement
    # (src = "s") tant que les candidatures ANR ne sont pas disponibles
    eff = effectifs_projets(lignes.rename(columns={"projet_id": "project_id"}).assign(src="s"),
                            projets.rename(columns={"projet_id": "project_id"}).assign(laureat=True),
                            ["annee", "discipline_code", "action_code"], pi=True, coordination=True, montant=True)
    eff = eff.rename(columns={"nb_laureats": "nb_projets", "nb_laureats_coordination": "nb_projets_coordination",
                              "nb_pi_laureats": "nb_participations", "montant_laureats": "montant_participations"})
    eff = eff[["entite_type", "entite_id", "annee", "discipline_code", "action_code", "nb_projets",
               "nb_projets_coordination", "nb_participations", "montant_participations"]]

    # Participations et projets (avec couverture du rattachement)
    rattachees = set(liens["ligne"])
    parts["rattachee"] = parts["ligne"].isin(rattachees)
    projets = projets.merge(parts.groupby("projet_id").agg(
        nb_participations=("participation_id", "nunique"),
        nb_participations_rattachees=("rattachee", "sum")).reset_index(), on="projet_id", how="left")
    cols_p = ["projet_id", "type", "annee", "action_code", "action_libelle", "budget_total", "budget_finance",
              "source_montant", "topic_id",
              "topic_score", "aboutness_note", "topic_source", "discipline_code", "domaine_code", "doute", "nb_participations",
              "nb_participations_rattachees"]
    p_out = parts.rename(columns={"entities_id": "participant_id", "entities_name": "nom"})
    cols_pa = ["projet_id", "participation_id", "role", "coordinateur", "participant_id", "type_identifiant",
               "nom", "pays_code", "montant", "source_montant", "ror", "nb_tutelles_rnsr", "rattachee"]
    tables = {"anr_projets": projets[cols_p], "anr_participations": p_out[cols_pa], "anr_effectifs": eff}

    # Entités présentes : nom (Paysage, sinon libellé scanR) et pays (participations), règle commune
    tot = (lignes.groupby(["entite_type", "entite_id"])["projet_id"].nunique()
           .rename("nb_projets").reset_index())
    liens_attrs = liens.merge(parts[["ligne", "projet_id", "entities_name", "pays_code"]], on="ligne")
    tot = nommer_entites(tot, liens_attrs, lien, ref_dir, pays="source", cle="projet_id")
    tot["pays_code"] = pays_entite(tot["pays_code"])
    tables["anr_entites"] = tot[["entite_id", "entite_type", "nom", "pays_code", "nb_projets"]] \
        .sort_values(["entite_type", "nb_projets"], ascending=[False, False])
    with duckdb.connect() as con:
        for nom, df in tables.items():
            con.register("t", df)
            con.execute(f"COPY t TO '{out / (nom + '.parquet')}' (FORMAT parquet, COMPRESSION zstd)")
            con.unregister("t")

    (non_rattaches(parts[~parts["rattachee"]], "projet_id", "nb_projets", cle_nom_si_vide=True)
     .to_csv(out / "anr_non_rattaches.csv", sep=";", index=False, encoding="utf-8"))

    src = projets["topic_source"].fillna("non classé").value_counts().to_dict()
    meta = {
        "source": {"scanr": str(f_scanr), "date_fichier": dt.datetime.fromtimestamp(
            f_scanr.stat().st_mtime).date().isoformat(), "types": list(types)},
        "date_calcul": dt.date.today().isoformat(),
        "nomenclature_disciplinaire": nomenclature,
        "annees": sorted(int(a) for a in projets["annee"].dropna().unique()),
        "parametres": {"rattachement_lignee": bool(rattachement_lignee), "topics": "aboutness",
                       "tous_partenaires": True, "aboutness_complet": aboutness_complet},
        "controles": {
            "nb_projets": int(len(projets)), "nb_participations": int(len(parts)),
            "pct_participations_identifiees": round(100 * parts["entities_id"].notna().mean(), 1),
            "pct_participations_rattachees": round(100 * parts["rattachee"].mean(), 1),
            "pct_projets_avec_etablissement": round(100 * (projets["nb_participations_rattachees"] > 0).mean(), 1),
            "liens_etablissement": liens["origine"].value_counts().to_dict(),
            "topic_source": src,
            "pct_projets_classes": round(100 * projets["discipline_code"].notna().mean(), 1),
            "coherence_scanr": controle,
            "montants_scanr": controle_montants,
        },
        "methode": [
            "Projets ANR financés de scanR ; tous les partenaires comptent (le coordinateur est gardé à part).",
            "Établissements : structure du partenaire (successeur Paysage, ROR) et toutes les tutelles de ses "
            "laboratoires RNSR, puis leurs tutelles OpenAlex, comme pour les bourses ERC, les prix et les articles.",
            "Panel ERC : topic du titre et du résumé du projet par le classifieur OpenAlex (aboutness), "
            "pour tous les projets (lauréats et candidatures comparables), rattaché au panel par la même "
            "table que les articles.",
        ],
    }
    (out / "anr_meta.json").write_text(json.dumps(meta, indent=1, ensure_ascii=False))
    if verbose:
        c = meta["controles"]
        print(f"ANR : {c['nb_projets']:,} projets ; participations identifiées {c['pct_participations_identifiees']} %, "
              f"rattachées à un établissement {c['pct_participations_rattachees']} % ; projets avec au moins un "
              f"établissement {c['pct_projets_avec_etablissement']} % ; panel : {c['pct_projets_classes']} % "
              f"(sources {src})")
    return {"dossier": str(out), "fichiers": sorted(str(out / f"{n}.parquet") for n in tables),
            "alerte_scanr": controle["alerte"], "complet": aboutness_complet, **meta["controles"]}
