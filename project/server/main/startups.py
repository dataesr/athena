"""Pilote Innovation : créations annuelles de startups liées à l'ESR."""

import datetime as dt
import hashlib
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .affiliations import rattacher_structures, tutelles_ror
from .config import (
    DATA_DIR,
    ODS_BASE_URL,
    REFERENTIEL_DIR,
    REFERENTIEL_SOURCE,
    STARTUPS_ESR_DATASET,
)
from .referentiel import charger_referentiel
from .utils_ods import get_ods_data

ODS_DATASET = STARTUPS_ESR_DATASET
ODS_URL = f"{ODS_BASE_URL}/explore/dataset/{ODS_DATASET}/"
ANNEE_DEBUT = 2012
SIREN_PATTERN = re.compile(r"^\d{9}$")
SIRET_PATTERN = re.compile(r"^\d{14}$")
COLONNES_OBLIGATOIRES = {
    "siren_startup",
    "date_creation_startup",
    "structures_liees_id",
    "observation_etat",
}


def _texte(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def _sirets(value) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        values = value
    elif isinstance(value, str):
        texte = value.strip()
        if texte.startswith("["):
            try:
                values = json.loads(texte)
            except json.JSONDecodeError:
                values = re.split(r"[|;,]", texte)
        else:
            values = re.split(r"[|;,]", texte)
    else:
        return set()
    return {
        texte
        for value in values
        if (texte := _texte(str(value).strip().strip("[]'\"")))
        and SIRET_PATTERN.fullmatch(texte)
    }


def _lignes_startups(frame):
    manquantes = COLONNES_OBLIGATOIRES.difference(frame.columns)
    if manquantes:
        raise ValueError(f"Colonnes startups ESR absentes : {sorted(manquantes)}")

    observation = pd.to_datetime(frame["observation_etat"], dayfirst=True, errors="coerce")
    observations = sorted({value.date().isoformat() for value in observation.dropna()})
    if not observations:
        raise ValueError("Aucune date d'observation valide dans observation_etat")

    sirens_absents = 0
    sirens_invalides = 0
    sirens = []
    for value in frame["siren_startup"]:
        texte = _texte(value) if isinstance(value, str) else None
        if not texte:
            sirens.append(None)
            sirens_absents += 1
        elif not SIREN_PATTERN.fullmatch(texte):
            sirens.append(None)
            sirens_invalides += 1
        else:
            sirens.append(texte)

    dates = pd.to_datetime(frame["date_creation_startup"], errors="coerce")
    annees = dates.dt.year
    dates_absentes = int(dates.isna().sum())
    lignes = pd.DataFrame({
        "siren": sirens,
        "annee": annees,
        "sirets": frame["structures_liees_id"].map(_sirets),
    })
    lignes = lignes[lignes["siren"].notna()]

    enregistrements = []
    sirens_contradictoires = set()
    sirens_sans_date = set()
    for siren, groupe in lignes.groupby("siren", sort=True):
        annees_valides = sorted({int(value) for value in groupe["annee"].dropna()})
        if len(annees_valides) > 1:
            sirens_contradictoires.add(siren)
            continue
        if not annees_valides:
            sirens_sans_date.add(siren)
            continue
        enregistrements.append({
            "siren": siren,
            "annee": annees_valides[0],
            "sirets": sorted(set().union(*groupe["sirets"].tolist())),
        })

    controles = {
        "nb_enregistrements_source": len(frame),
        "nb_siren_absents": sirens_absents,
        "nb_siren_invalides": sirens_invalides,
        "nb_dates_creation_absentes": dates_absentes,
        "nb_siren_dates_contradictoires": len(sirens_contradictoires),
        "nb_siren_sans_date": len(sirens_sans_date),
        "nb_siren_avant_2012": sum(row["annee"] < ANNEE_DEBUT for row in enregistrements),
        "nb_siren_dates_apres_observation": sum(
            row["annee"] > dt.date.fromisoformat(observations[-1]).year for row in enregistrements
        ),
    }
    enregistrements = [
        row for row in enregistrements
        if ANNEE_DEBUT <= row["annee"] <= dt.date.fromisoformat(observations[-1]).year
    ]
    return enregistrements, observations, controles


def _ecrire_parquet(frame, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("frame", frame)
        con.execute(f"COPY frame TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
    return path


def _source_hash(frame):
    values = pd.util.hash_pandas_object(frame.astype("string").fillna(""), index=False)
    return hashlib.sha256(values.values.tobytes()).hexdigest()


def _entites(rors, referentiel, ref_dir, data_dir):
    referentiel_par_ror = {}
    if referentiel is not None and not referentiel.empty and "ror" in referentiel.columns:
        referentiel_par_ror = referentiel.drop_duplicates("ror").set_index("ror").to_dict("index")

    noms_par_ror = {}
    if REFERENTIEL_SOURCE != "scanr":
        scanr = charger_referentiel("scanr", ref_dir)
        if scanr is not None and not scanr.empty:
            noms_par_ror = scanr.drop_duplicates("ror").set_index("ror")["nom"].to_dict()
    institutions = Path(data_dir) / "institutions.parquet"
    if institutions.exists():
        with duckdb.connect() as con:
            noms = con.execute(
                "SELECT ror, any_value(display_name) AS nom FROM read_parquet(?) GROUP BY ror",
                [str(institutions)],
            ).df()
        for row in noms.itertuples(index=False):
            if _texte(row.nom) and not _texte(noms_par_ror.get(row.ror)):
                noms_par_ror[row.ror] = row.nom

    lignes = []
    for ror in sorted(rors):
        ref = referentiel_par_ror.get(ror, {})
        nom = _texte(ref.get("nom")) or _texte(noms_par_ror.get(ror)) or ror
        sigle = _texte(ref.get("sigle"))
        ville = _texte(ref.get("ville"))
        recherche = " ".join(str(value) for value in (nom, sigle, ville, ror) if value).lower()
        lignes.append({
            "entite_id": ror,
            "entite_type": "etablissement",
            "nom": nom,
            "sigle": sigle,
            "ville": ville,
            "recherche": recherche,
        })
    return pd.DataFrame(lignes, columns=[
        "entite_id", "entite_type", "nom", "sigle", "ville", "recherche",
    ])


def compute_startups(data_dir: str = DATA_DIR, ref_dir: str = REFERENTIEL_DIR,
                     verbose: bool = True) -> dict:
    """Calcule INN-STU-01 et écrit les agrégats et contrôles locaux."""
    frame = get_ods_data(ODS_DATASET, strict=True, dtype=str)
    if frame.empty:
        raise ValueError(f"Le jeu ODS {ODS_DATASET} ne contient aucun enregistrement")
    date_extraction = dt.date.today().isoformat()
    dossier = Path(data_dir) / "innovation"
    dossier.mkdir(parents=True, exist_ok=True)
    enregistrements, observations, controles = _lignes_startups(frame)
    _ecrire_parquet(frame, dossier / "startups_brut.parquet")
    sirets = sorted({siret for row in enregistrements for siret in row["sirets"]})
    lignee_disponible = all(
        (Path(data_dir) / fichier).exists()
        for fichier in ("inst_lineage.parquet", "institutions.parquet")
    )
    par_siret = {}
    sirets_avec_tutelles = set()
    if sirets:
        structures = pd.DataFrame({"entities_id": sirets, "rnsr": None})
        structures, liens = rattacher_structures(structures, ref_dir)
        if not liens.empty:
            lien = tutelles_ror(liens["ror"], data_dir)
            rattachements = liens.merge(structures[["ligne", "entities_id"]], on="ligne").merge(lien, on="ror")
            par_siret = rattachements.groupby("entities_id")["entite_id"].agg(set).to_dict()
            sirets_avec_tutelles = set(
                rattachements.loc[rattachements["ror"] != rattachements["entite_id"], "entities_id"]
            )

    paires = set()
    non_rattaches = []
    for row in enregistrements:
        rors_startup = set()
        if not row["sirets"]:
            non_rattaches.append({
                "siren": row["siren"], "annee": row["annee"], "siret": None,
                "motif": "aucun_siret_valide",
            })
        for siret in row["sirets"]:
            if siret in par_siret:
                rors_startup.update(par_siret[siret])
            else:
                non_rattaches.append({
                    "siren": row["siren"], "annee": row["annee"], "siret": siret,
                    "motif": "absent",
                })
        paires.update((ror, row["siren"], row["annee"]) for ror in rors_startup)

    annees = sorted({row["annee"] for row in enregistrements})
    if not annees:
        raise ValueError(f"Aucune date de création exploitable à partir de {ANNEE_DEBUT}")
    annees_completes = list(range(ANNEE_DEBUT, max(annees) + 1))
    annee_provisoire = max(annees)
    compte_france = {
        annee: sum(row["annee"] == annee for row in enregistrements)
        for annee in annees_completes
    }
    rors = sorted({ror for ror, _, _ in paires})
    compte_etablissements = {
        (ror, annee): sum(pair_ror == ror and pair_annee == annee for pair_ror, _, pair_annee in paires)
        for ror in rors for annee in annees_completes
    }

    lignes_effectifs = []
    for entite_type, entite_id, compte in [
        ("pays", "FR", compte_france),
        *[
            ("etablissement", ror, {annee: compte_etablissements[(ror, annee)] for annee in annees_completes})
            for ror in rors
        ],
    ]:
        for annee in annees_completes:
            lignes_effectifs.append({
                "indicateur_id": "INN-STU-01",
                "entite_type": entite_type,
                "entite_id": entite_id,
                "annee": annee,
                "discipline_code": None,
                "nb_startups": int(compte[annee]),
                "unite": "nombre",
                "statut": "provisoire" if annee == annee_provisoire else "disponible",
            })
    effectifs = pd.DataFrame(lignes_effectifs)

    nb_sirets_rattaches = len(par_siret)
    controles.update({
        "nb_siren_comptes": len(enregistrements),
        "nb_siren_sans_siret_valide": sum(not row["sirets"] for row in enregistrements),
        "nb_sirets_distincts": len(sirets),
        "nb_sirets_rattaches": nb_sirets_rattaches,
        "nb_sirets_avec_tutelles_openalex": len(sirets_avec_tutelles),
        "nb_sirets_non_resolus": len(sirets) - nb_sirets_rattaches,
        "nb_siren_sans_ror_resolu": len({
            row["siren"] for row in enregistrements
            if not any(
                siret in par_siret
                for siret in row["sirets"]
            )
        }),
    })
    referentiel = charger_referentiel(REFERENTIEL_SOURCE, ref_dir)
    liste_entites = _entites(rors, referentiel, ref_dir, data_dir)
    nb_noms_absents = int((liste_entites["nom"] == liste_entites["entite_id"]).sum())
    controles["nb_entites_sans_nom"] = nb_noms_absents
    if nb_noms_absents:
        print(f"Startups ESR : {nb_noms_absents} établissements sans nom connu, ROR affiché.")
    meta = {
        "indicateur_id": "INN-STU-01",
        "annees": annees_completes,
        "annees_provisoires": [annee_provisoire],
        "date_calcul": date_extraction,
        "methode": [
            "Nombre de SIREN distincts par année de création, à partir de 2012.",
            "France : chaque SIREN valide compte une fois, y compris sans structure liée exploitable.",
            "Établissement : règle commune d'affiliation, SIRET Paysage puis SIREN si nécessaire, avec successeurs Paysage.",
            ("Les tutelles de la lignée OpenAlex sont ajoutées." if lignee_disponible
             else "Lignée OpenAlex absente : les tutelles OpenAlex ne sont pas ajoutées."),
            "Les SIRET sans ROR résolu par la règle commune ne sont pas attribués à un établissement.",
            "Une startup liée à plusieurs ROR peut compter une fois dans chacun ; les comptes par établissement ne s'additionnent pas.",
            "Le recensement est incomplet et rétrospectif ; une absence de startup recensée ne prouve pas l'absence de création.",
            f"L'année {annee_provisoire} est provisoire.",
        ],
        "source": {
            "dataset": ODS_DATASET,
            "url": ODS_URL,
            "licence": "Licence Ouverte v2.0",
            "date_extraction": date_extraction,
            "date_observation": observations[-1],
            "empreinte_sha256": _source_hash(frame),
        },
        "rattachement": {
            "source": "règle commune d'affiliation : Paysage, tutelles scanR et lignée OpenAlex",
            "date_extraction": date_extraction,
            "referentiel_noms": REFERENTIEL_SOURCE,
            "lignee_openalex_disponible": lignee_disponible,
        },
        "controles": controles,
    }

    _ecrire_parquet(effectifs, dossier / "startups_effectifs.parquet")
    _ecrire_parquet(liste_entites, dossier / "startups_entites.parquet")
    _ecrire_parquet(pd.DataFrame(non_rattaches, columns=["siren", "annee", "siret", "motif"]),
                    dossier / "startups_non_rattaches.parquet")
    (dossier / "startups_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if verbose:
        print(
            f"Startups ESR : {len(enregistrements):,} SIREN comptés, "
            f"{len(rors):,} ROR liés, {nb_sirets_rattaches:,}/{len(sirets):,} SIRET rattachés "
            f"-> {dossier}"
        )
    return {
        "fichiers": [
            str(dossier / "startups_effectifs.parquet"),
            str(dossier / "startups_entites.parquet"),
            str(dossier / "startups_meta.json"),
            str(dossier / "startups_non_rattaches.parquet"),
        ],
        "nb_siren": len(enregistrements),
        "nb_entites": len(rors),
        "nb_sirets_rattaches": nb_sirets_rattaches,
        "annees": annees_completes,
    }
