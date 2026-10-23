"""
publish.py
==========
Étape 7 : publier les données pour l'application tableaux
(https://github.com/dataesr/tableaux), en fichiers puis, si on le souhaite,
dans sa base MongoDB.

Le contrat (noms et contenu de chaque collection) est décrit dans
CONTRAT_DONNEES.md. Résumé :

  entites                  une ligne par entité (monde, pays, établissement), commune
                           à tous les onglets ; clé entite_id (ROR court, code ISO, MONDE)
  biblio_disciplines       disciplines du classement et grands domaines
  biblio_effectifs         nombres d'articles par périmètre x entité x année x discipline
                           (l'entité MONDE donne les dénominateurs)
  biblio_copublications    articles cosignés par entité x partenaire x année x discipline
  biblio_revues_tete       revues de tête de chaque discipline
  biblio_seuils_top10      seuil de citations du top 10 % par discipline x année
  biblio_meta              un document : snapshot, paramètres, textes de méthode

1. publish()          écrit <DATA_DIR>/publication/<classement>/<collection>.jsonl.gz
                      (+ .parquet) et manifest.json. Aucune base nécessaire.
2. publish_mongo()    charge ces fichiers dans MongoDB, avec la convention de
                      tableaux : une collection <collection>_<horodatage> par
                      version, et la version ajoutée dans le document du tableau
                      (collection « boards », id = TABLEAUX_BOARD_ID). Les index
                      utiles aux routes du serveur sont créés.

Exemple :
    from project.server.main.publish import publish, publish_mongo
    res = publish()                    # fichiers
    publish_mongo()                    # base : MONGO_URI (variable d'environnement)
    publish_mongo(collections=["entites"])   # une seule collection
"""

import datetime as dt
import gzip
import json
import time
from pathlib import Path

import duckdb
import pandas as pd

from .common import DOMAINES_NOMS
from .config import (CLASSEMENT, DASHBOARD_ETABS_PATH, DATA_DIR, MONGO_DB, MONGO_URI,
                     REFERENTIEL_DIR, REFERENTIEL_SOURCE, SJR_PATH, TABLEAUX_BOARD_ID)
from .entites import construire_entites

COLLECTIONS = ["entites", "biblio_disciplines", "biblio_effectifs", "biblio_copublications",
               "biblio_revues_tete", "biblio_seuils_top10", "biblio_meta"]

# Index créés dans Mongo (champs dans l'ordre des filtres des routes)
INDEX = {
    "entites": [[("entite_id", 1)], [("selecteur", 1), ("ordre_selecteur", 1)], [("entite_type", 1)]],
    "biblio_disciplines": [[("classement", 1), ("discipline_code", 1)]],
    "biblio_effectifs": [[("entite_id", 1), ("perimetre", 1), ("classement", 1),
                          ("annee_publication", 1), ("discipline_code", 1)],
                         [("entite_type", 1), ("perimetre", 1), ("annee_publication", 1)]],
    "biblio_copublications": [[("entite_id", 1), ("partenaire_type", 1), ("perimetre", 1),
                               ("annee_publication", 1)],
                              [("partenaire_id", 1)]],
    "biblio_revues_tete": [[("classement", 1), ("discipline_code", 1), ("categorie", 1), ("rang", 1)]],
    "biblio_seuils_top10": [[("perimetre", 1), ("classement", 1), ("discipline_code", 1),
                             ("annee_publication", 1)]],
    "biblio_meta": [],
}


def _json(p: Path) -> dict:
    return json.loads(p.read_text()) if p.exists() else {}


def methode_textes(p_ind: dict, p_top: dict, p_rt: dict) -> list:
    """Textes de la note méthodologique, tirés des paramètres réellement utilisés."""
    w = p_top.get("citation_window")
    first = (p_top.get("counts_years") or [None])[0]
    types = ", ".join(p_top.get("work_types") or []) or "tous types"
    multi = p_rt.get("revues_multidisciplinaires_retenues") or []
    return [
        f"Périmètre : documents OpenAlex de type « {types} » parus dans une revue, "
        f"publiés de {p_ind.get('annee_min')} à {p_ind.get('annee_max')} ; chaque article est classé "
        f"dans une seule discipline (topic principal OpenAlex -> panel ERC).",
        "Comptage entier : un article compte pour un pays s'il a au moins un auteur de ce pays "
        "(outre-mer compris pour la France), et pour un établissement si l'une de ses affiliations "
        "est cet établissement ou l'une de ses structures rattachées (lignée OpenAlex, ROR).",
        "Périmètre de référence : soit toutes les revues OpenAlex, soit les seules revues indexées "
        "dans SJR (proche du périmètre Scopus). Le choix modifie le poids et l'influence, pas le prestige.",
        (f"Influence : part de l'entité dans les {round(100 * (p_top.get('top_share') or .1))} % "
         f"d'articles les plus cités de chaque discipline et année. Citations reçues l'année de "
         f"publication et les {w} années suivantes"
         + (f" (années antérieures à {first} : citations cumulées)" if first else "")
         + " ; articles jamais cités exclus du top."
         if w is not None else
         "Influence : part dans le top 10 % des articles les plus cités (citations cumulées)."),
        "Top 10 % selon la méthode de Waltman et Schreiber (2013) : il contient exactement 10 % des "
        "articles de chaque discipline et année, compté en équivalent-articles (d'où des décimales) ; "
        "les articles à égalité au seuil de citations se partagent les places restantes au prorata "
        "(3 ex aequo pour 1 place : 1/3 chacun). Réf. : L. Waltman, M. Schreiber, « On the calculation "
        "of percentile-based bibliometric indicators », JASIST 64(2), 2013, p. 372-379.",
        "Années provisoires : la fenêtre de citation n'est pas encore close, l'influence peut évoluer.",
        (f"Prestige : part de l'entité dans les articles d'une discipline parus dans ses "
         f"{p_rt.get('nb_revues_tete', 10)} revues de tête (SJR le plus élevé parmi les revues dont au "
         f"moins {round(100 * (p_rt.get('min_share') or 0))} % des articles relèvent de la discipline, "
         f"hors revues de synthèse)"
         + (f", et, en option, dans les revues multidisciplinaires {', '.join(multi)}" if multi else "")
         + ". Un article ne compte que pour sa propre discipline."),
        "Agrégation : pour une période ou un ensemble de disciplines, les effectifs sont additionnés "
        "avant le calcul des parts (pas de moyenne de pourcentages).",
    ]


def meta_bibliometrie(classement: str = CLASSEMENT, data_dir: str = DATA_DIR) -> dict:
    """Document biblio_meta : snapshot, paramètres de chaque étape, textes de méthode."""
    data_dir = Path(data_dir)
    p_ind = _json(data_dir / "indicateurs" / f"{classement}_params.json")
    perims = p_ind.get("perimetres") or []
    p_top = _json(data_dir / "top10" / f"{classement}_{perims[0]}_params.json") if perims else {}
    p_rt = _json(data_dir / "revues_tete" / f"{classement}_params.json")
    p_cop = _json(data_dir / "indicateurs" / f"{classement}_copublications_params.json")
    man = _json(data_dir / "works_manifest.json")
    snapshot = {"source": man.get("source", "public"), "date": man.get("date")}
    y0, y1 = p_ind.get("annee_min"), p_ind.get("annee_max")
    W, last = p_top.get("citation_window"), (p_top.get("counts_years") or [None, None])[1]
    prov = [a for a in range(y0, y1 + 1) if W is not None and last and a + W >= last] if y0 else []
    calcul = dt.date.today().isoformat()
    return {
        "classement": classement,
        "snapshot_openalex": snapshot,
        "date_calcul": calcul,
        "perimetres": perims,
        "annees": list(range(y0, y1 + 1)) if y0 else [],
        "annees_influence_provisoire": prov,
        "nb_revues_tete": p_rt.get("nb_revues_tete"),
        "revues_multidisciplinaires": p_rt.get("revues_multidisciplinaires_retenues") or [],
        "pays_compares": p_ind.get("pays_compares") or [],
        "parametres": {"top10": p_top, "revues_tete": p_rt, "effectifs": p_ind, "copublications": p_cop},
        "methode": methode_textes(p_ind, p_top, p_rt),
        "sources": (f"Sources : OpenAlex (snapshot {snapshot['source']} du {snapshot['date']}), "
                    f"SCImago Journal Rank ({Path(p_rt.get('sjr_path') or SJR_PATH).name}). "
                    f"Indicateurs calculés le {calcul}."),
    }


def _tables(classement: str, data_dir: Path, etabs_path: str, verbose: bool,
            referentiel_source: str = REFERENTIEL_SOURCE, referentiel_dir: str = REFERENTIEL_DIR) -> dict:
    """Chaque collection sous forme de DataFrame (ou de dict pour biblio_meta)."""
    ind, rt, t10 = data_dir / "indicateurs", data_dir / "revues_tete", data_dir / "top10"
    sql = lambda q: duckdb.sql(q).df()
    meta = meta_bibliometrie(classement, data_dir)
    tables = {"entites": construire_entites(classement, data_dir, etabs_path, referentiel_source,
                                            referentiel_dir, verbose=verbose)}

    disc = sql(f"""SELECT DISTINCT '{classement}' AS classement, discipline_code, discipline_nom, domaine_code
                   FROM read_parquet('{data_dir}/disciplines/{classement}.parquet')
                   WHERE discipline_code IS NOT NULL""")
    disc["domaine_nom"] = disc["domaine_code"].map(DOMAINES_NOMS).fillna(disc["domaine_code"])
    tables["biblio_disciplines"] = disc.sort_values("discipline_code", key=lambda s: s.map(_cle_code))

    tables["biblio_effectifs"] = sql(f"SELECT * FROM read_parquet('{ind}/{classement}_effectifs.parquet')")
    f_cop = ind / f"{classement}_copublications.parquet"
    if f_cop.exists():
        tables["biblio_copublications"] = sql(f"SELECT * FROM read_parquet('{f_cop}')")
    elif verbose:
        print(f"{f_cop} absent : biblio_copublications non publiée (lancer compute_copublications())")
    tables["biblio_revues_tete"] = sql(f"""SELECT '{classement}' AS classement, *
                                           FROM read_parquet('{rt}/{classement}.parquet')""")
    seuils = []
    for perim in meta["perimetres"]:
        f = t10 / f"{classement}_{perim}_seuils.parquet"
        if f.exists():
            seuils.append(sql(f"SELECT '{perim}' AS perimetre, '{classement}' AS classement, * "
                              f"FROM read_parquet('{f}')"))
    if seuils:
        tables["biblio_seuils_top10"] = pd.concat(seuils, ignore_index=True)
    tables["biblio_meta"] = meta
    return tables


def _cle_code(code: str):
    """Tri naturel des codes : LS1, LS2, ..., LS9, PE1, ..., PE10, PE11, SH1..."""
    import re
    m = re.match(r"([A-Za-z]*)(\d*)(.*)", str(code))
    return (m.group(1), int(m.group(2)) if m.group(2) else 0, m.group(3))


def publish(classement: str = CLASSEMENT, data_dir: str = DATA_DIR,
            etabs_path: str = DASHBOARD_ETABS_PATH, out_dir: str = None,
            parquet: bool = True, referentiel_source: str = REFERENTIEL_SOURCE,
            referentiel_dir: str = REFERENTIEL_DIR, verbose: bool = True) -> dict:
    """Écrit chaque collection dans <out_dir> (défaut <data_dir>/publication/<classement>) :
    <collection>.jsonl.gz (un document JSON par ligne, prêt pour Mongo) et, si parquet=True,
    <collection>.parquet ; plus manifest.json (liste, nombre de lignes, colonnes).
    Renvoie le manifeste."""
    data_dir = Path(data_dir)
    out = Path(out_dir) if out_dir else data_dir / "publication" / classement
    out.mkdir(parents=True, exist_ok=True)
    tables = _tables(classement, data_dir, etabs_path, verbose, referentiel_source, referentiel_dir)
    manifeste = {"classement": classement, "genere_le": dt.datetime.now().isoformat(timespec="seconds"),
                 "snapshot_openalex": tables["biblio_meta"]["snapshot_openalex"], "collections": {}}
    for nom, t in tables.items():
        f = out / f"{nom}.jsonl.gz"
        if isinstance(t, dict):                               # biblio_meta : un seul document
            with gzip.open(f, "wt", encoding="utf-8") as fh:
                fh.write(json.dumps(t, ensure_ascii=False) + "\n")
            manifeste["collections"][nom] = {"nb_documents": 1, "fichier": f.name}
            (out / f"{nom}.json").write_text(json.dumps(t, ensure_ascii=False, indent=1), encoding="utf-8")
            continue
        # entiers à valeurs manquantes (Int64) : écrits en entiers ou null, pas en décimaux
        t = t.copy()
        for c in t.columns:
            if str(t[c].dtype) == "Int64":
                t[c] = t[c].astype(object).where(t[c].notna(), None)
        with gzip.open(f, "wt", encoding="utf-8") as fh:
            t.to_json(fh, orient="records", lines=True, force_ascii=False)
        info = {"nb_documents": len(t), "fichier": f.name,
                "colonnes": {c: str(t[c].dtype) for c in t.columns}}
        if parquet:
            with duckdb.connect() as con:
                con.register("t", t)
                con.execute(f"COPY t TO '{out / (nom + '.parquet')}' (FORMAT parquet, COMPRESSION zstd)")
        manifeste["collections"][nom] = info
    (out / "manifest.json").write_text(json.dumps(manifeste, ensure_ascii=False, indent=1), encoding="utf-8")
    if verbose:
        print(f"\nPublication -> {out}")
        for nom, i in manifeste["collections"].items():
            print(f"  {nom:24} {i['nb_documents']:>12,} documents")
    return manifeste


def _lire_jsonl(f: Path, taille: int):
    lot = []
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for ligne in fh:
            lot.append(json.loads(ligne))
            if len(lot) >= taille:
                yield lot
                lot = []
    if lot:
        yield lot


def publish_mongo(classement: str = CLASSEMENT, data_dir: str = DATA_DIR, uri: str = MONGO_URI,
                  db_name: str = MONGO_DB, board_id: str = TABLEAUX_BOARD_ID, collections=None,
                  pub_dir: str = None, enregistrer_version: bool = True, taille_lot: int = 20000,
                  verbose: bool = True) -> dict:
    """Charge les fichiers de publish() dans MongoDB (convention de tableaux).

    Chaque collection est écrite dans une NOUVELLE collection <nom>_<horodatage> :
    l'ancienne version reste en place tant que le tableau ne pointe pas sur la
    nouvelle. Si enregistrer_version=True, la version est ajoutée dans le document
    du tableau (collection « boards », id = board_id, entrée data.id = <nom>), comme
    le fait la page d'administration de tableaux (/admin/upload-version).
    uri : défaut = variable d'environnement MONGO_URI (jamais en dur dans le code).
    Renvoie {collection: nom de la version créée}."""
    try:
        from pymongo import MongoClient
    except ImportError as e:
        raise ImportError("pymongo manquant : ajouter « pymongo » à requirements.txt") from e
    if not uri:
        raise ValueError("MONGO_URI non défini (variable d'environnement)")
    src = Path(pub_dir) if pub_dir else Path(data_dir) / "publication" / classement
    man = _json(src / "manifest.json")
    if not man:
        raise FileNotFoundError(f"{src}/manifest.json absent : lancer publish() d'abord")
    noms = collections or list(man["collections"])
    client = MongoClient(uri)
    db = client[db_name]
    horodatage = int(time.time() * 1000)            # millisecondes, comme tableaux
    res = {}
    for nom in noms:
        f = src / man["collections"][nom]["fichier"]
        version = f"{nom}_{horodatage}"
        n = 0
        for lot in _lire_jsonl(f, taille_lot):
            db[version].insert_many(lot, ordered=False)
            n += len(lot)
        for cles in INDEX.get(nom, []):
            db[version].create_index(cles)
        res[nom] = version
        if verbose:
            print(f"{nom:24} {n:>12,} documents -> {db_name}.{version}")
        if enregistrer_version:
            entree = {"id": version, "createdAt": horodatage}
            r = db["boards"].update_one({"id": board_id, "data": {"$elemMatch": {"id": nom}}},
                                        {"$push": {"data.$.versions": entree}})
            if not r.matched_count:
                r = db["boards"].update_one({"id": board_id},
                                            {"$push": {"data": {"id": nom, "versions": [entree]}}})
                if not r.matched_count and verbose:
                    print(f"   tableau « {board_id} » absent de {db_name}.boards : version non "
                          f"enregistrée (le créer dans l'application tableaux)")
    client.close()
    return res
