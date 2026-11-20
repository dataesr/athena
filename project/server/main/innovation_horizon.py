"""Pilote Innovation : projets Horizon Europe Pilier 2 à convention signée."""

import datetime as dt
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd

from .affiliations import rattacher_structures, tutelles_ror
from .common import pays_entite
from .config import DATA_DIR, HORIZON_ODS_LAUREATS, ODS_BASE_URL, REFERENTIEL_DIR, REFERENTIEL_SOURCE
from .horizon import lire_horizon, participations
from .referentiel import charger_referentiel
from .startups import _entites

COLONNES_REQUISES = {
    "project_id", "call_year", "pilier_code", "status_code", "entities_id",
    "entities_id_source", "entities_name", "country_code",
}


def compute_innovation_horizon(data_dir: str = DATA_DIR, ref_dir: str = REFERENTIEL_DIR,
                              telecharger: bool = True, verbose: bool = True) -> dict:
    """Calcule INN-PRJ-01 pour la France et ses établissements, toutes disciplines."""
    data_dir = Path(data_dir)
    horizon_dir = data_dir / "horizon"
    dossier = data_dir / "innovation"
    brut = lire_horizon("laureats", str(horizon_dir), telecharger)
    manquantes = COLONNES_REQUISES.difference(brut.columns)
    if manquantes:
        raise ValueError(f"Colonnes Horizon absentes : {sorted(manquantes)}")

    pilier = brut[brut["pilier_code"].fillna("").str.strip().str.upper() == "HORIZON.2"].copy()
    if pilier.empty:
        raise ValueError("Aucune participation Horizon Europe Pilier 2 dans la source")
    pilier["project_id"] = pilier["project_id"].fillna("").str.strip()
    pilier["status_code"] = pilier["status_code"].fillna("").str.strip().str.upper()
    pilier["call_year"] = pilier["call_year"].fillna("").str.strip()
    nb_sans_projet = int((pilier["project_id"] == "").sum())
    avec_id = pilier[pilier["project_id"] != ""].copy()
    projets_signes = set(avec_id.loc[avec_id["status_code"] == "SIGNED", "project_id"])
    signes = avec_id[avec_id["project_id"].isin(projets_signes)].copy()
    statuts = signes[signes["status_code"] != ""].groupby("project_id")["status_code"].nunique()
    annees = signes[signes["call_year"] != ""].groupby("project_id")["call_year"].nunique()
    conflits = set(statuts[statuts > 1].index) | set(annees[annees > 1].index)
    if conflits:
        raise ValueError(f"{len(conflits)} projets Pilier 2 SIGNED ont des statuts ou années d'appel contradictoires")

    annees_projet = (signes[signes["call_year"].str.fullmatch(r"\d{4}")]
                     .drop_duplicates("project_id").set_index("project_id")["call_year"])
    sans_annee = projets_signes - set(annees_projet.index)
    signes = signes[signes["project_id"].isin(annees_projet.index)].copy()
    signes["call_year"] = signes["project_id"].map(annees_projet)
    if signes.empty:
        raise ValueError("Aucun projet Pilier 2 SIGNED avec une année d'appel exploitable")

    parts = participations(signes, "laureats")
    parts["pays_code"] = pays_entite(parts["pays_code"])
    parts_fr = parts[parts["pays_code"] == "FR"].copy()
    if parts_fr.empty:
        raise ValueError("Aucune participation française aux projets Pilier 2 SIGNED retenus")

    parts_fr["identifiant_source"] = parts_fr["entities_id"]
    source_absente = parts_fr["entities_id_source"].isna()
    numerique_neuf = parts_fr["entities_id"].fillna("").str.fullmatch(r"\d{9}")
    parts_fr["identifiant_ambigu"] = source_absente & numerique_neuf
    parts_fr.loc[parts_fr["identifiant_ambigu"], "entities_id"] = None
    parts_fr, liens = rattacher_structures(parts_fr, ref_dir)

    lignee_disponible = all(
        (data_dir / nom).exists() for nom in ("inst_lineage.parquet", "institutions.parquet")
    )
    rattachees = set(liens["ligne"])
    parts_fr["rattachee"] = parts_fr["ligne"].isin(rattachees)
    non_rattaches = parts_fr.loc[~parts_fr["rattachee"], [
        "project_id", "participation_id", "identifiant_source", "entities_id_source",
        "entities_name", "pays_code", "identifiant_ambigu", "type_identifiant",
    ]]

    annee_par_projet = {projet: int(annee) for projet, annee in annees_projet.items()}
    france = parts_fr[["project_id"]].drop_duplicates()
    france["annee"] = france["project_id"].map(annee_par_projet)
    france["entite_id"] = "FR"
    france["entite_type"] = "pays"
    lignes = [france]
    if not liens.empty:
        lien = tutelles_ror(liens["ror"], data_dir)
        etablissements = (liens.merge(parts_fr[["ligne", "project_id"]], on="ligne")
                          .merge(lien, on="ror")[["project_id", "entite_id"]].drop_duplicates())
        etablissements["annee"] = etablissements["project_id"].map(annee_par_projet)
        etablissements["entite_type"] = "etablissement"
        lignes.append(etablissements)
    rattachements = pd.concat(lignes, ignore_index=True).drop_duplicates()
    effectifs = (rattachements.groupby(["entite_type", "entite_id", "annee"])["project_id"]
                 .nunique().rename("nb_projets").reset_index())
    effectifs["indicateur_id"] = "INN-PRJ-01"
    effectifs["discipline_code"] = None
    effectifs["unite"] = "nombre"
    effectifs["statut"] = "provisoire"
    effectifs = effectifs[["indicateur_id", "entite_type", "entite_id", "annee",
                           "discipline_code", "nb_projets", "unite", "statut"]]

    rors = sorted(effectifs.loc[effectifs["entite_type"] == "etablissement", "entite_id"].unique())
    referentiel = charger_referentiel(REFERENTIEL_SOURCE, ref_dir)
    entites = _entites(rors, referentiel, ref_dir, data_dir)
    date_calcul = dt.date.today().isoformat()
    empreinte = hashlib.sha256(pd.util.hash_pandas_object(
        brut.astype("string").fillna(""), index=False
    ).values.tobytes()).hexdigest()
    meta = {
        "indicateur_id": "INN-PRJ-01",
        "libelle": "Projets Horizon Europe Pilier 2 à convention signée",
        "date_calcul": date_calcul,
        "annees": sorted(int(a) for a in effectifs["annee"].unique()),
        "annees_provisoires": sorted(int(a) for a in effectifs["annee"].unique()),
        "source": {
            "dataset": HORIZON_ODS_LAUREATS,
            "url": f"{ODS_BASE_URL}/explore/dataset/{HORIZON_ODS_LAUREATS}/",
            "licence": "Licence Ouverte v2.0",
            "date_calcul": date_calcul,
            "telechargement": telecharger,
            "empreinte_sha256": empreinte,
        },
        "rattachement": {
            "source": "règle commune d'affiliation : Paysage, tutelles scanR et lignée OpenAlex",
            "lignee_openalex_disponible": lignee_disponible,
            "referentiel_noms": REFERENTIEL_SOURCE,
        },
        "controles": {
            "nb_lignes_sans_projet": nb_sans_projet,
            "nb_projets_signes_sans_annee_valide": len(sans_annee),
            "nb_projets_signes": len(projets_signes),
            "nb_projets_france": int(france["project_id"].nunique()),
            "nb_lignes_identifiants_sans_pays": int(parts["pays_code"].isna().sum()),
            "nb_lignes_identifiants_france": len(parts_fr),
            "nb_lignes_identifiants_france_non_rattachees": int((~parts_fr["rattachee"]).sum()),
            "nb_identifiants_numeriques_source_absente": int(parts_fr["identifiant_ambigu"].sum()),
            "nb_etablissements_sans_nom": int((entites["nom"] == entites["entite_id"]).sum()),
            "statuts_lignes_pilier_2": pilier["status_code"].replace("", "(vide)").value_counts().to_dict(),
            "roles_lignes_identifiants_france": parts_fr["participates_as"].fillna("(vide)").value_counts().to_dict(),
        },
        "methode": [
            "Projets Horizon Europe Pilier 2 au statut SIGNED ; tous les rôles de participation sont inclus.",
            "Un projet distinct compte une fois par année d'appel pour la France et chaque établissement rattaché.",
            "La France est calculée directement à partir des participations françaises, outre-mer inclus.",
            "Les établissements incluent les structures et tutelles de la règle commune ; leurs comptes ne sont pas additifs.",
            "Les identifiants numériques à neuf chiffres sans source ne sont pas résolus par hypothèse.",
            "Les années sont provisoires : le périmètre des statuts reste à valider et les données peuvent évoluer.",
            ("Les tutelles OpenAlex sont prises en compte." if lignee_disponible
             else "Lignée OpenAlex absente : les tutelles OpenAlex ne sont pas ajoutées."),
            "Toutes disciplines ; aucun montant financier n'est calculé.",
        ],
    }

    dossier.mkdir(parents=True, exist_ok=True)
    tables = {"horizon_effectifs": effectifs, "horizon_entites": entites,
              "horizon_non_rattaches": non_rattaches}
    fichiers = []
    with duckdb.connect() as con:
        for nom, table in tables.items():
            path = dossier / f"{nom}.parquet"
            con.register("table_sortie", table)
            con.execute(f"COPY table_sortie TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
            con.unregister("table_sortie")
            fichiers.append(str(path))
    path_meta = dossier / "horizon_meta.json"
    path_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    fichiers.append(str(path_meta))
    if verbose:
        print(f"Innovation Horizon : {len(france):,} projets France, {len(rors):,} établissements -> {dossier}")
    return {"fichiers": fichiers, "nb_projets_france": len(france), "nb_etablissements": len(rors)}
