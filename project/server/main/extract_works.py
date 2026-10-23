"""
extract_works.py
================
Étape 1 : extraction légère des works OpenAlex (snapshot Parquet).

Fonction principale : extract_works(...)
Pour chaque fichier Parquet du snapshot (liste tirée du manifest, téléchargé
automatiquement s'il manque), on lit UNIQUEMENT les sous-colonnes utiles, on filtre, et on écrit
deux petits fichiers locaux :

  <data_dir>/works/<partition>__<part>.parquet      une ligne par article
      work_id, year, type, topic_id, source_id, source_type, issn_l, issn,
      cited_by_count, counts_by_year, oa_pct, oa_top10, countries

  <data_dir>/work_inst/<partition>__<part>.parquet  une ligne par (article, institution)
      work_id, inst_id

Filtres appliqués dès l'extraction :
  - publication_year >= annee_min
  - type dans `types`
  - non rétracté, non paratexte
  - primary_topic présent (sinon pas de discipline)
Le filtre "source de type journal" n'est PAS appliqué : on garde source_type.

Reprise sur erreur : un fichier déjà extrait est sauté, on peut relancer.

Exemple :
    from project.server.main.extract_works import extract_works, summary
    res = extract_works()                                   # tout le snapshot, depuis 2015
    res = extract_works(test=True)                          # un seul fichier
    summary()                                               # volumes par année

Nouveau snapshot OpenAlex
-------------------------
Chaque snapshot est extrait dans son propre dossier, l'ancien est conservé.
Source réglée par SNAPSHOT_SOURCE dans config.py :
  "daily"  (offre payante, clé OPENALEX_API_KEY) : un snapshot complet par jour
  "public" : le snapshot trimestriel actuel seulement
    published_snapshot()                  # snapshots disponibles chez OpenAlex
    list_snapshots()                      # snapshots déjà extraits localement
    extract_snapshot()                    # le plus récent -> /data/snapshots/<date>/
    extract_snapshot("2026-10-14")        # un snapshot quotidien précis
Puis, dans config.py : SNAPSHOT = "<date>", et relancer les étapes 3 à 6.
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

from .config import DATA_DIR, DATA_ROOT, PAYS_FR, SNAPSHOT_SOURCE
from .openalex_s3 import connect, connect_for, list_daily_snapshots, readable_url, snapshot_manifest




def fetch_manifest(manifest_path: str = None, source: str = SNAPSHOT_SOURCE,
                   date: str = None) -> dict:
    """Liste des fichiers works du snapshot (source "public" ou "daily", voir
    openalex_s3) ; l'enregistre dans manifest_path si fourni."""
    manifest = snapshot_manifest("works", source=source, date=date)
    if manifest_path is None:
        return manifest
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=1))
    return manifest


def _short(expr: str) -> str:
    """'https://openalex.org/W123' -> 'W123' (robuste si déjà court)."""
    return f"regexp_extract({expr}, '[^/]+$')"


def _sql_extract(url: str, annee_min: int, types: tuple) -> str:
    """Une seule lecture du fichier distant -> table temporaire t.
    On ne cite que des sous-champs précis pour que DuckDB ne lise que ces
    colonnes 'feuilles' du Parquet."""
    types_sql = ", ".join(f"'{t}'" for t in types)
    # Lecture d'un fichier Parquet OpenAlex (un fichier = quelques centaines de
    # milliers d'articles), en une seule passe :
    #   - id, primary_topic.id, primary_location.source.id sont des URL
    #     (https://openalex.org/W123) : _short() garde la fin (W123) ;
    #   - `countries` : un article a une liste d'auteurs (authorships) ; chaque
    #     auteur a ses pays (a.countries) et ses institutions, qui ont chacune un
    #     pays (i.country_code). list_transform construit, par auteur, la liste
    #     « ses pays + pays de ses institutions » ; flatten met bout à bout les
    #     listes de tous les auteurs ; list_distinct dédoublonne ; list_filter
    #     retire les NULL. Exemple :
    #       auteur 1 : countries [FR], institutions [CNRS (FR)]  -> [FR, FR]
    #       auteur 2 : countries [],   institutions [MIT (US)]   -> [US]
    #       flatten -> [FR, FR, US] ; list_distinct -> [FR, US]
    #   - `inst_ids` : même principe avec les identifiants des institutions ;
    #     la liste est ensuite « dépliée » (unnest) en une ligne par
    #     (article, institution) dans work_inst/.
    #   - WHERE : années, types, et on écarte les articles rétractés, le
    #     « paratexte » (sommaires, éditoriaux de couverture...) et les articles
    #     sans topic (impossibles à classer dans une discipline).
    # On ne cite que des sous-champs précis : DuckDB ne lit que ces colonnes du
    # Parquet distant (environ 4 % du volume du fichier).
    return f"""
    CREATE OR REPLACE TEMP TABLE t AS
    SELECT
        {_short('id')}                                  AS work_id,
        publication_year::SMALLINT                      AS year,
        type,
        {_short('primary_topic.id')}                    AS topic_id,
        {_short('primary_location.source.id')}          AS source_id,
        primary_location.source.type                    AS source_type,
        primary_location.source.issn_l                  AS issn_l,
        primary_location.source.issn                    AS issn,
        cited_by_count,
        counts_by_year,
        citation_normalized_percentile.value            AS oa_pct,
        citation_normalized_percentile.is_in_top_10_percent AS oa_top10,
        -- pays : champ 'countries' de chaque auteur + country_code de ses institutions
        list_filter(
            list_distinct(flatten(list_transform(authorships, a ->
                list_concat(a.countries,
                            list_transform(a.institutions, i -> i.country_code))
            ))),
            c -> c IS NOT NULL
        )                                               AS countries,
        -- institutions (ids OpenAlex) de tous les auteurs, dédoublonnées
        list_filter(
            list_distinct(flatten(list_transform(authorships, a ->
                list_transform(a.institutions, i -> {_short('i.id')})
            ))),
            x -> x IS NOT NULL AND x <> ''
        )                                               AS inst_ids
    FROM read_parquet('{url}')
    WHERE publication_year >= {annee_min}
      AND type IN ({types_sql})
      AND NOT coalesce(is_retracted, false)
      AND NOT coalesce(is_paratext, false)
      AND primary_topic.id IS NOT NULL
    """


def _out_name(s3_url: str) -> str:
    # .../works/updated_date=2026-06-24/part_0000.parquet -> updated_date=2026-06-24__part_0000.parquet
    parts = s3_url.split("/")
    return f"{parts[-2]}__{parts[-1]}"


def extract_one(s3_url: str, out_works: Path, out_inst: Path,
                annee_min: int = 2015, types: tuple = ("article", "review"),
                retries: int = 3) -> tuple:
    """Extrait un fichier du snapshot. Renvoie (nom, nb_lignes, secondes);
    nb_lignes = -1 si le fichier était déjà extrait."""
    name = _out_name(s3_url)
    f_works, f_inst = Path(out_works) / name, Path(out_inst) / name
    if f_works.exists() and f_inst.exists():
        return name, -1, 0.0

    url = readable_url(s3_url)
    for attempt in range(1, retries + 1):
        try:
            t0 = time.time()
            con = connect_for(s3_url)
            con.execute(_sql_extract(url, annee_min, types))
            n = con.execute("SELECT count(*) FROM t").fetchone()[0]
            # écriture atomique : .tmp puis renommage
            tmp_w, tmp_i = f_works.with_suffix(".tmp"), f_inst.with_suffix(".tmp")
            # Deux sorties : works/ (une ligne par article, sans la liste d'institutions)
            # et work_inst/ (une ligne par article x institution : unnest « déplie »
            # la liste, W1 [I1, I2] -> (W1, I1), (W1, I2))
            con.execute(f"COPY (SELECT * EXCLUDE (inst_ids) FROM t) "
                        f"TO '{tmp_w}' (FORMAT parquet, COMPRESSION zstd)")
            con.execute(f"COPY (SELECT work_id, unnest(inst_ids) AS inst_id FROM t) "
                        f"TO '{tmp_i}' (FORMAT parquet, COMPRESSION zstd)")
            con.close()
            tmp_w.rename(f_works)
            tmp_i.rename(f_inst)
            return name, n, time.time() - t0
        except Exception as e:  # réseau, timeout S3...
            if attempt == retries:
                raise RuntimeError(f"{name} : échec après {retries} essais : {e}") from e
            time.sleep(5 * attempt)


def extract_works(annee_min: int = 2015,
                  types: tuple = ("article", "review"),
                  data_dir: str = DATA_DIR,
                  manifest_path: str = None,
                  workers: int = 8,
                  test: bool = False,
                  limit: int = None,
                  refresh_manifest: bool = False,
                  verbose: bool = True) -> dict:
    """Extrait les works du snapshot vers <data_dir>/works et <data_dir>/work_inst.

    annee_min     : première année de publication gardée
    types         : types OpenAlex gardés
    data_dir      : dossier de travail
    manifest_path : manifest des works (défaut : <data_dir>/works_manifest.json) ;
                    téléchargé automatiquement s'il n'existe pas
    workers       : nombre de fichiers traités en parallèle
    test          : True -> un seul fichier (le plus gros)
    limit         : ne traiter que les `limit` premiers fichiers (essais)
    refresh_manifest : True -> re-télécharger le manifest (nouveau snapshot trimestriel)
    verbose       : afficher la progression

    Renvoie {'fichiers', 'extraits', 'deja_faits', 'lignes', 'erreurs', 'minutes'}.
    """
    data_dir = Path(data_dir)
    out_works, out_inst = data_dir / "works", data_dir / "work_inst"
    out_works.mkdir(parents=True, exist_ok=True)
    out_inst.mkdir(parents=True, exist_ok=True)
    connect(install=True).close()  # installe httpfs une fois, avant les threads

    manifest_path = Path(manifest_path) if manifest_path else data_dir / "works_manifest.json"
    deja = list(out_works.glob("*.parquet"))
    if (refresh_manifest or not manifest_path.exists()) and deja:
        # Garde-fou : ne jamais compléter une extraction existante avec la liste de
        # fichiers d'un AUTRE snapshot (les deux se mélangeraient, avec des doublons)
        raise RuntimeError(
            f"{data_dir} contient déjà {len(deja)} fichiers extraits"
            + ("" if manifest_path.exists() else " mais pas de works_manifest.json")
            + " : pour un nouveau snapshot, utiliser extract_snapshot() (nouveau dossier) ; "
              "pour reprendre une extraction interrompue, relancer sans refresh_manifest.")
    if refresh_manifest or not manifest_path.exists():
        manifest = fetch_manifest(manifest_path)
        if verbose:
            print(f"Manifest téléchargé : snapshot du {manifest.get('date')}, "
                  f"{len(manifest['files'])} fichiers -> {manifest_path}")
    manifest = json.loads(manifest_path.read_text())
    files = manifest["files"]
    if test:
        files = [max(files, key=lambda f: f["meta"].get("record_count") or f["meta"].get("content_length", 0))]
    elif limit:
        files = files[:limit]
    urls = [f["url"] for f in files]

    if verbose:   # état avant de commencer : ce qui est déjà là, ce qui reste à lire sur S3
        noms = [_out_name(u) for u in urls]
        complets = sum((out_works / n).exists() and (out_inst / n).exists() for n in noms)
        partiels = sum((out_works / n).exists() != (out_inst / n).exists() for n in noms)
        print(f"Dossier {data_dir} — snapshot {manifest.get('source', 'public')} du {manifest.get('date')} : "
              f"{len(urls)} fichiers, {complets} déjà extraits, {len(urls) - complets} à lire sur S3"
              + (f" (dont {partiels} extraits à moitié, interrompus en cours d'écriture)" if partiels else ""))

    t_start = time.time()
    res = {"fichiers": len(urls), "extraits": 0, "deja_faits": 0, "lignes": 0, "erreurs": []}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(extract_one, u, out_works, out_inst, annee_min, tuple(types))
                   for u in urls]
        for k, fut in enumerate(as_completed(futures), 1):
            try:
                name, n, dt = fut.result()
                if n < 0:
                    res["deja_faits"] += 1
                    msg = "déjà extrait"
                else:
                    res["extraits"] += 1
                    res["lignes"] += n
                    msg = f"{n:>8,} lignes en {dt:5.1f} s"
            except Exception as e:
                res["erreurs"].append(str(e))
                name, msg = "", f"ERREUR {e}"
            if verbose:
                print(f"[{k}/{len(urls)}] {name}  {msg}")

    res["minutes"] = round((time.time() - t_start) / 60, 1)
    if verbose:
        print(f"Terminé en {res['minutes']} min — {res['extraits']} extraits, "
              f"{res['deja_faits']} déjà faits, {len(res['erreurs'])} erreur(s).")
    return res


def check_duplicates(data_dir: str = DATA_DIR) -> dict:
    """Chaque work ne doit apparaître qu'une fois dans <data_dir>/works."""
    n, n_distinct = duckdb.connect().execute(f"""
        SELECT count(*), count(DISTINCT work_id) FROM read_parquet('{data_dir}/works/*.parquet')
    """).fetchone()
    return {"n_works": n, "doublons": n - n_distinct}


def summary(data_dir: str = DATA_DIR):
    """Volumes extraits par année (DataFrame) : total, articles, % en revue,
    % avec ≥1 pays, nb avec ≥1 auteur France (métropole + outre-mer)."""
    data_dir = Path(data_dir)
    fr = ", ".join(f"'{c}'" for c in PAYS_FR)
    return duckdb.connect().execute(f"""
        SELECT year,
               count(*)                                                    AS total,
               count(*) FILTER (WHERE type = 'article')                    AS articles,
               round(100 * avg((source_type = 'journal')::INT), 1)         AS pct_journal,
               round(100 * avg((len(countries) > 0)::INT), 1)              AS pct_avec_pays,
               count(*) FILTER (WHERE list_bool_or(
                   list_transform(countries, c -> c IN ({fr}))))           AS france
        FROM read_parquet('{data_dir}/works/*.parquet')
        GROUP BY year ORDER BY year
    """).df()


# --- Gestion des snapshots ---------------------------------------------------------

def published_snapshot(source: str = SNAPSHOT_SOURCE) -> dict:
    """Snapshots disponibles chez OpenAlex.
    "public" : le snapshot trimestriel actuel (date, fichiers, works).
    "daily"  : la liste des snapshots quotidiens (offre payante) et le plus récent."""
    if source == "daily":
        dates = list_daily_snapshots()
        return {"source": "daily", "dernier": dates[-1] if dates else None, "dates": dates}
    m = fetch_manifest(source="public")
    return {"source": "public", "date": m.get("date"), "fichiers": len(m["files"]),
            "works": sum(f["meta"].get("record_count") or 0 for f in m["files"])}


def _snapshot_dirs(data_root: str = DATA_ROOT) -> dict:
    """{date: dossier} des extractions locales (dossier racine compris)."""
    root, out = Path(data_root), {}
    candidats = [root] + sorted((root / "snapshots").glob("*"))
    for d in candidats:
        m = d / "works_manifest.json"
        if m.exists() and (d / "works").exists():
            out[json.loads(m.read_text()).get("date")] = d
    return out


def list_snapshots(data_root: str = DATA_ROOT):
    """Snapshots extraits localement, avec l'état de chaque étape."""
    import pandas as pd
    rows = []
    for date, d in sorted(_snapshot_dirs(data_root).items(), key=lambda x: str(x[0])):
        rows.append({
            "snapshot": date,
            "dossier": str(d),
            "SNAPSHOT (config)": None if d == Path(data_root) else d.name,
            "fichiers_works": len(list((d / "works").glob("*.parquet"))),
            "institutions": (d / "institutions.parquet").exists(),
            "disciplines": (d / "disciplines").exists(),
            "top10": (d / "top10").exists(),
            "revues_tete": (d / "revues_tete").exists(),
            "indicateurs": (d / "indicateurs").exists(),
            "publication": (d / "publication").exists(),
        })
    return pd.DataFrame(rows)


def _link_or_copy(src: Path, dst: Path) -> None:
    try:
        os.link(src, dst)          # lien physique : pas d'espace disque en plus
    except OSError:
        import shutil
        shutil.copy2(src, dst)


def extract_snapshot(date: str = None,
                     source: str = SNAPSHOT_SOURCE,
                     data_root: str = DATA_ROOT,
                     reuse_from: str = "auto",
                     annee_min: int = 2015,
                     types: tuple = ("article", "review"),
                     workers: int = 8,
                     reference_tables: bool = True,
                     verbose: bool = True) -> dict:
    """Extrait un snapshot OpenAlex dans <data_root>/snapshots/<date>/.

    source "public" : le snapshot trimestriel actuel (date ignorée / vérifiée).
    source "daily"  : offre payante (OPENALEX_API_KEY) ; date = "AAAA-MM-JJ",
                      défaut : le snapshot quotidien le plus récent
                      (liste : published_snapshot("daily")).

    Les snapshots précédents ne sont pas touchés. Pour gagner du temps, les
    fichiers identiques à ceux d'un snapshot déjà extrait (même nom, même
    taille, même nombre de lignes dans le manifest, mêmes paramètres
    d'extraction) sont repris par lien physique au lieu d'être relus sur S3.
    Les citations changeant sans cesse, une grande partie des fichiers est
    néanmoins nouvelle à chaque snapshot.

    reuse_from       : dossier d'un snapshot déjà extrait ; "auto" = le plus récent ;
                       None = tout ré-extraire
    reference_tables : lance aussi extract_sources_institutions (étape 2) sur
                       le nouveau dossier
    Ensuite : SNAPSHOT = "<date>" dans config.py, puis étapes 3 à 6.
    """
    manifest = fetch_manifest(source=source, date=date)
    date = manifest.get("date")
    target = Path(data_root) / "snapshots" / str(date)
    p_params = target / "extract_params.json"
    if p_params.exists():
        prev = json.loads(p_params.read_text()).get("source", "public")
        if prev != source:
            raise ValueError(f"{target} contient déjà un snapshot de source {prev!r}")
    (target / "works").mkdir(parents=True, exist_ok=True)
    (target / "work_inst").mkdir(parents=True, exist_ok=True)
    mpath = target / "works_manifest.json"
    mpath.write_text(json.dumps(manifest, indent=1))
    params = {"annee_min": annee_min, "types": list(types), "source": source}
    p_params.write_text(json.dumps(params))
    if verbose:
        print(f"Snapshot {source} du {date} : {len(manifest['files'])} fichiers -> {target}")

    # Reprise des fichiers inchangés depuis un snapshot local
    reused, src_dir = 0, None
    if reuse_from == "auto":
        autres = {d: p for d, p in _snapshot_dirs(data_root).items() if p != target}
        src_dir = autres[max(autres, key=str)] if autres else None
    elif reuse_from:
        src_dir = Path(reuse_from)
    if src_dir is not None:
        p_old = src_dir / "extract_params.json"
        old_params = json.loads(p_old.read_text()) if p_old.exists() else None
        if old_params not in (None, params):
            if verbose:
                print(f"Paramètres d'extraction différents de {src_dir} : rien n'est repris")
        else:
            old = json.loads((src_dir / "works_manifest.json").read_text())
            old_meta = {_out_name(f["url"]): f["meta"] for f in old["files"]}
            for f in manifest["files"]:
                name = _out_name(f["url"])
                src_w, src_i = src_dir / "works" / name, src_dir / "work_inst" / name
                dst_w, dst_i = target / "works" / name, target / "work_inst" / name
                if (old_meta.get(name) == f["meta"] and src_w.exists() and src_i.exists()
                        and not dst_w.exists()):
                    _link_or_copy(src_w, dst_w)
                    _link_or_copy(src_i, dst_i)
                    reused += 1
            if verbose:
                print(f"{reused} fichiers inchangés repris de {src_dir} "
                      f"(snapshot {old.get('date')}) ; {len(manifest['files']) - reused} à extraire")

    res = extract_works(annee_min=annee_min, types=types, data_dir=str(target),
                        manifest_path=str(mpath), workers=workers, verbose=verbose)
    res.update(snapshot=date, source=source, dossier=str(target), repris=reused,
               repris_de=str(src_dir) if src_dir else None)
    res.update(check_duplicates(str(target)))
    if verbose and res["doublons"]:
        print(f"ATTENTION : {res['doublons']:,} work_id en double dans {target}")

    if reference_tables:
        from .extract_sources_institutions import extract_sources_institutions
        res["references"] = extract_sources_institutions(data_dir=str(target), verbose=verbose)

    if verbose:
        print(f"\nPour utiliser ce snapshot : SNAPSHOT = \"{date}\" dans config.py, "
              f"puis relancer les étapes 3 à 6 (disciplines, revues de tête, top 10 %, indicateurs).")
    return res
