"""
extract_sources_institutions.py
===============================
Étape 2 : les deux tables de référence, petites (quelques centaines de milliers
de lignes), lues en entier depuis le snapshot Parquet.

Fonction principale : extract_sources_institutions(...)

Sorties (dans data_dir) :
  sources.parquet
      source_id, display_name, type, issn_l, issn, issn_norm
      -> issn_norm = tous les ISSN (issn + issn_l) normalisés sans tiret,
         en majuscules ('1542-4863' -> '15424863'), pour l'appariement SJR.

  institutions.parquet
      inst_id, ror, display_name, country_code, type, lineage,
      display_name_acronyms, display_name_alternatives (sigles, noms alternatifs)
      -> ror au format court ('https://ror.org/02feahw73' -> '02feahw73')
      -> lineage = l'institution + toutes ses institutions parentes (ids OpenAlex)

  inst_lineage.parquet          <- la table clé du filtre établissement
      inst_id, anc_inst_id, anc_ror
      une ligne par (institution, ancêtre), l'institution étant son propre ancêtre.
      Exemple : un labo UMR -> lui-même, son université, le CNRS.
      Ainsi "les articles de l'établissement R" =
        work_inst ⨝ inst_lineage sur inst_id, où anc_ror = R.

Exemple :
    from project.server.main.extract_sources_institutions import extract_sources_institutions
    res = extract_sources_institutions()
    res["top_fr"]          # contrôle de la lignée (DataFrame)
"""

import json
from pathlib import Path

import duckdb

from .config import DATA_DIR, SNAPSHOT_SOURCE
from .openalex_s3 import connect, readable_url, snapshot_manifest


def _snapshot_of(data_dir: str) -> tuple:
    """(source, date) du snapshot extrait dans data_dir (lu dans works_manifest.json),
    pour lire les revues et institutions du MÊME snapshot que les articles."""
    m = Path(data_dir) / "works_manifest.json"
    if not m.exists():
        return SNAPSHOT_SOURCE, None
    man = json.loads(m.read_text())
    return man.get("source", "public"), man.get("date")


def file_urls(entity: str, data_dir: str = DATA_DIR, verbose: bool = True) -> list:
    """Fichiers Parquet d'une entité, dans le snapshot correspondant à data_dir."""
    source, date = _snapshot_of(data_dir)
    man = snapshot_manifest(entity, source=source, date=date if source == "daily" else None)
    urls = [readable_url(f["url"]) for f in man["files"]]
    if verbose:
        n = sum(f["meta"].get("record_count") or 0 for f in man["files"])
        print(f"{entity} ({source} {man.get('date')}) : {len(urls)} fichiers"
              + (f", {n:,} enregistrements" if n else ""))
    return urls


def _sql_list(urls: list) -> str:
    return "[" + ", ".join(f"'{u}'" for u in urls) + "]"


def _short(expr: str) -> str:
    return f"regexp_extract({expr}, '[^/]+$')"


def extract_sources(con, data_dir: str = DATA_DIR, verbose: bool = True):
    """Table des revues avec ISSN normalisés -> <data_dir>/sources.parquet.
    Renvoie le décompte par type de source (DataFrame)."""
    # issn_norm : tous les ISSN de la revue (issn + issn_l), normalisés pour
    # l'appariement avec SJR, qui les écrit sans tiret :
    #   issn ['1476-4687', '0028-0836'], issn_l '0028-0836'
    #   list_concat           -> ['1476-4687', '0028-0836', '0028-0836']
    #   list_transform        -> ['14764687', '00280836', '00280836'] (sans tiret, majuscules : X final)
    #   list_filter           -> garde les chaînes de 8 caractères (écarte NULL et valeurs mal formées)
    #   list_distinct         -> ['14764687', '00280836']
    con.execute(f"""
        CREATE OR REPLACE TABLE sources AS
        SELECT
            {_short('id')}  AS source_id,
            display_name,
            type,
            issn_l,
            issn,
            list_distinct(list_filter(
                list_transform(list_concat(issn, [issn_l]),
                               x -> upper(replace(x, '-', ''))),
                x -> x IS NOT NULL AND length(x) = 8
            )) AS issn_norm
        FROM read_parquet({_sql_list(file_urls('sources', data_dir, verbose))})
    """)
    con.execute(f"COPY sources TO '{data_dir}/sources.parquet' (FORMAT parquet, COMPRESSION zstd)")
    return con.execute("""
        SELECT type, count(*) AS n, count(*) FILTER (WHERE len(issn_norm) > 0) AS avec_issn
        FROM sources GROUP BY type ORDER BY n DESC
    """).df()


def extract_institutions(con, data_dir: str = DATA_DIR, verbose: bool = True) -> None:
    """Institutions (ROR, pays, lignée) -> <data_dir>/institutions.parquet,
    puis lignée dépliée -> <data_dir>/inst_lineage.parquet."""
    urls = _sql_list(file_urls('institutions', data_dir, verbose))
    # Sigles et noms alternatifs (recherche dans le dashboard : « CNRS », « Inserm »...),
    # seulement si le snapshot les contient
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({urls})").fetchall()}
    extra = "".join(
        f",\n            {c}" if c in cols else f",\n            []::VARCHAR[] AS {c}"
        for c in ("display_name_acronyms", "display_name_alternatives"))
    con.execute(f"""
        CREATE OR REPLACE TABLE institutions AS
        SELECT
            {_short('id')}  AS inst_id,
            {_short('ror')} AS ror,
            display_name,
            country_code,
            type,
            list_transform(lineage, x -> {_short('x')}) AS lineage{extra}
        FROM read_parquet({urls})
    """)
    con.execute(f"COPY institutions TO '{data_dir}/institutions.parquet' (FORMAT parquet, COMPRESSION zstd)")

    # Lignée « dépliée » : une ligne par (institution, ancêtre), l'institution
    # étant son propre ancêtre. OpenAlex donne déjà, pour chaque institution, la
    # liste COMPLÈTE de ses tutelles (directes et indirectes) dans `lineage`.
    # Exemple : UMR 7590 (I3), lineage = [I2 Sorbonne Université, I1 CNRS]
    #   list_concat([I3], [I2, I1]) = [I3, I2, I1] ; unnest -> 3 lignes :
    #     (I3, I3, ror UMR), (I3, I2, ror Sorbonne), (I3, I1, ror CNRS)
    # Le filtre établissement de l'étape 6 lit cette table à l'envers : « tous
    # les articles des institutions dont l'ancêtre a le ROR X » = les articles de X
    # et de toutes ses structures rattachées.
    # LEFT JOIN : on récupère le ROR de l'ancêtre (NULL s'il n'en a pas).
    con.execute("""
        CREATE OR REPLACE TABLE inst_lineage AS
        WITH pairs AS (
            SELECT inst_id, unnest(list_concat([inst_id], lineage)) AS anc_inst_id
            FROM institutions
        )
        SELECT DISTINCT p.inst_id, p.anc_inst_id, i.ror AS anc_ror
        FROM pairs p
        LEFT JOIN institutions i ON i.inst_id = p.anc_inst_id
        WHERE p.anc_inst_id IS NOT NULL AND p.anc_inst_id <> ''
    """)
    con.execute(f"COPY inst_lineage TO '{data_dir}/inst_lineage.parquet' (FORMAT parquet, COMPRESSION zstd)")


def controls(con) -> dict:
    """Contrôles sur les tables institutions / inst_lineage déjà en mémoire."""
    n, avec_ror, fr = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE ror IS NOT NULL AND ror <> ''),
               count(*) FILTER (WHERE country_code = 'FR')
        FROM institutions
    """).fetchone()
    dup = con.execute("""
        SELECT count(*) FROM (SELECT ror FROM institutions
                              WHERE ror IS NOT NULL AND ror <> ''
                              GROUP BY ror HAVING count(*) > 1)
    """).fetchone()[0]
    top_fr = con.execute("""
        SELECT l.anc_ror AS ror, i.display_name, count(*) - 1 AS nb_descendants
        FROM inst_lineage l JOIN institutions i ON i.inst_id = l.anc_inst_id
        WHERE i.country_code = 'FR'
        GROUP BY 1, 2 ORDER BY nb_descendants DESC LIMIT 10
    """).df()
    return {"institutions": n, "avec_ror": avec_ror, "fr": fr,
            "ror_dupliques": dup, "top_fr": top_fr}


def extract_sources_institutions(data_dir: str = DATA_DIR, verbose: bool = True) -> dict:
    """Produit sources.parquet, institutions.parquet et inst_lineage.parquet
    dans data_dir, et renvoie les contrôles :
      {'sources_par_type': DataFrame, 'institutions', 'avec_ror', 'fr',
       'ror_dupliques', 'top_fr': DataFrame}"""
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    con = connect(install=True, daily=_snapshot_of(data_dir)[0] == "daily")
    res = {"sources_par_type": extract_sources(con, data_dir, verbose)}
    extract_institutions(con, data_dir, verbose)
    res.update(controls(con))
    con.close()
    if verbose:
        print(res["sources_par_type"].to_string(index=False), "\n")
        print(f"Institutions : {res['institutions']:,} | avec ROR : {res['avec_ror']:,} "
              f"| FR : {res['fr']:,} | ROR partagés : {res['ror_dupliques']}")
        print("\nInstitutions FR ayant le plus de « descendants » (contrôle de la lignée) :")
        print(res["top_fr"].to_string(index=False))
    return res
