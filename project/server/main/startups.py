"""Pilote Innovation : créations annuelles de startups liées à l'ESR."""

import datetime as dt
import hashlib
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .config import (
    DATA_DIR,
    ODS_BASE_URL,
    REFERENTIEL_DIR,
    REFERENTIEL_SOURCE,
    SCANR_ORGANIZATIONS_URL,
    STARTUPS_ESR_DATASET,
)
from .referentiel import _lignes_jsonl_gz, charger_referentiel
from .utils_ods import get_ods_data

ODS_DATASET = STARTUPS_ESR_DATASET
ODS_URL = f"{ODS_BASE_URL}/explore/dataset/{ODS_DATASET}/"
ANNEE_DEBUT = 2012
SIREN_PATTERN = re.compile(r"^\d{9}$")
SIRET_PATTERN = re.compile(r"^\d{14}$")
ROR_PATTERN = re.compile(r"^0[a-z0-9]{6}[0-9]{2}$")
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


def _ror(value):
    texte = _texte(value)
    if not texte:
        return None
    identifiant = texte.rstrip("/").split("/")[-1].lower()
    return identifiant if ROR_PATTERN.fullmatch(identifiant) else None


def _label(organisation):
    labels = organisation.get("label") or {}
    return _texte(labels.get("fr")) or _texte(labels.get("default")) or _texte(labels.get("en"))


def _external_ids(organisation, type_id):
    return {
        _texte(item.get("id"))
        for item in organisation.get("externalIds") or []
        if isinstance(item, dict) and str(item.get("type", "")).lower() == type_id
        and _texte(item.get("id"))
    }


def resolve_sirets(sirets, organisations):
    """Résout uniquement les SIRET ayant un unique ROR distinct dans scanR."""
    demandes = set(sirets)
    rors_par_siret = {siret: set() for siret in demandes}
    noms_par_ror = {}

    for organisation in organisations:
        siret_correspondant = _external_ids(organisation, "siret").intersection(demandes)
        if not siret_correspondant:
            continue
        rors = {_ror(value) for value in _external_ids(organisation, "ror")}
        rors.discard(None)
        for siret in siret_correspondant:
            rors_par_siret[siret].update(rors)
        nom = _label(organisation)
        for ror in rors:
            if nom:
                noms_par_ror.setdefault(ror, nom)

    correspondances = {}
    for siret, rors in rors_par_siret.items():
        statut = "unique" if len(rors) == 1 else "ambigu" if len(rors) > 1 else "absent"
        correspondances[siret] = {"rors": sorted(rors), "statut": statut}
    return correspondances, noms_par_ror


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


def _entites(rors, noms_par_ror, referentiel):
    referentiel_par_ror = {}
    if referentiel is not None and not referentiel.empty and "ror" in referentiel.columns:
        referentiel_par_ror = referentiel.drop_duplicates("ror").set_index("ror").to_dict("index")

    lignes = []
    for ror in sorted(rors):
        ref = referentiel_par_ror.get(ror, {})
        nom = ref.get("nom") or noms_par_ror.get(ror) or ror
        sigle = ref.get("sigle")
        ville = ref.get("ville")
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


def compute_startups(data_dir: str = DATA_DIR, raw_data=None, organisations=None,
                     referentiel=None, date_extraction: str = None, verbose: bool = True) -> dict:
    """Calcule INN-STU-01 et écrit uniquement des agrégats destinés à tableaux.

    `raw_data`, `organisations` et `referentiel` permettent de tester le calcul
    sans télécharger les sources.
    """
    frame = raw_data if raw_data is not None else get_ods_data(ODS_DATASET, strict=True, dtype=str)
    if frame.empty:
        raise ValueError(f"Le jeu ODS {ODS_DATASET} ne contient aucun enregistrement")
    date_extraction = date_extraction or dt.date.today().isoformat()
    dossier = Path(data_dir) / "innovation"
    dossier.mkdir(parents=True, exist_ok=True)
    enregistrements, observations, controles = _lignes_startups(frame)
    _ecrire_parquet(frame, dossier / "startups_brut.parquet")
    sirets = sorted({siret for row in enregistrements for siret in row["sirets"]})
    source_organisations = organisations
    if source_organisations is None:
        source_organisations = _lignes_jsonl_gz(SCANR_ORGANIZATIONS_URL) if sirets else []
    correspondances, noms_par_ror = resolve_sirets(sirets, source_organisations)

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
            resolution = correspondances[siret]
            if resolution["statut"] == "unique":
                rors_startup.update(resolution["rors"])
            else:
                non_rattaches.append({
                    "siren": row["siren"], "annee": row["annee"], "siret": siret,
                    "motif": resolution["statut"],
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

    liens_uniques = sum(value["statut"] == "unique" for value in correspondances.values())
    controles.update({
        "nb_siren_comptes": len(enregistrements),
        "nb_siren_sans_siret_valide": sum(not row["sirets"] for row in enregistrements),
        "nb_sirets_distincts": len(sirets),
        "nb_sirets_rattaches_unique": liens_uniques,
        "nb_sirets_ambigus": sum(value["statut"] == "ambigu" for value in correspondances.values()),
        "nb_sirets_non_resolus": sum(value["statut"] == "absent" for value in correspondances.values()),
        "nb_siren_sans_ror_resolu": len({
            row["siren"] for row in enregistrements
            if not any(
                correspondances[siret]["statut"] == "unique"
                for siret in row["sirets"]
            )
        }),
    })
    referentiel = referentiel if referentiel is not None else charger_referentiel(
        REFERENTIEL_SOURCE, REFERENTIEL_DIR
    )
    liste_entites = _entites(rors, noms_par_ror, referentiel)
    meta = {
        "indicateur_id": "INN-STU-01",
        "annees": annees_completes,
        "annees_provisoires": [annee_provisoire],
        "date_calcul": date_extraction,
        "methode": [
            "Nombre de SIREN distincts par année de création, à partir de 2012.",
            "France : chaque SIREN valide compte une fois, y compris sans structure liée exploitable.",
            "Établissement : un SIREN compte une fois par ROR lorsqu'au moins un SIRET lié se résout vers ce ROR unique.",
            "Les SIRET absents ou associés à plusieurs ROR distincts ne sont pas attribués à un établissement.",
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
            "source": "scanR organizations-v2",
            "date_extraction": date_extraction,
            "referentiel_noms": REFERENTIEL_SOURCE,
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
            f"{len(rors):,} ROR liés, {liens_uniques:,}/{len(sirets):,} SIRET résolus sans ambiguïté "
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
        "nb_sirets_rattaches_unique": liens_uniques,
        "annees": annees_completes,
    }
