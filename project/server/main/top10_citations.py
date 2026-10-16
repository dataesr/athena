"""
top10_citations.py
==================
Étape 4 : repérer les articles du top 10 % des plus cités de leur discipline
et de leur année.

Méthode
-------
1. Périmètre « monde » : articles extraits filtrés par config.py
   (WORK_TYPES, SOURCE_TYPES, ANNEE_MIN, ANNEE_MAX), avec une discipline.
2. Citations de chaque article :
   - CITATION_WINDOW = N : citations reçues de l'année de publication à N ans
     après, tirées de counts_by_year. OpenAlex ne garde que ~10 ans dans
     counts_by_year : une cohorte dont la fenêtre commence avant la première
     année couverte bascule sur cited_by_count cumulé. Comme le classement se
     fait à l'intérieur d'une année, ce changement de mesure ne biaise pas
     la comparaison entre articles d'une même cohorte.
   - CITATION_WINDOW = None : cited_by_count cumulé pour toutes les années.
3. Dans chaque cellule discipline x année (n articles), on classe par citations
   décroissantes. Pour un article à c citations, avec a articles strictement
   plus cités et b articles ex aequo :
        w_top = min(1, max(0, (TOP_SHARE * n - a) / b))
   -> les articles nettement au-dessus du seuil valent 1, ceux du seuil
      se partagent la place restante, et le total vaut exactement 10 % de n
      (méthode de Waltman & Schreiber, 2013).
   Avec TOP_EXCLUDE_UNCITED, un article jamais cité vaut toujours 0.

Sorties (dans <data_dir>/top10/)
--------------------------------
  <nom>.parquet          une ligne par article du périmètre
      work_id, year, discipline_code, citations, w_top
  <nom>_seuils.parquet   une ligne par discipline x année
      discipline_code, year, n, seuil, w_seuil, part_top, mode_citations, provisoire
  <nom>_params.json      paramètres utilisés (traçabilité)

Exemple :
    from project.server.main.top10_citations import compute_top10
    res = compute_top10()                              # paramètres de config.py
    res = compute_top10(disciplines="erc_alt")         # autre regroupement
    res = compute_top10(world="sjr")                   # monde = revues SJR -> erc_sjr.parquet
    res = compute_top10(citation_window=None, out_name="erc_cumul")
"""

import json
from pathlib import Path

from .common import connect, output_name, perimeter_where, world_sources
from .config import (ANNEE_MAX, ANNEE_MIN, CITATION_WINDOW, COUNTS_FIRST_YEAR,
                     DATA_DIR, DISCIPLINES_NAME, SOURCE_TYPES, TOP_EXCLUDE_UNCITED,
                     TOP_SHARE, WORK_TYPES, WORLD)


def detect_counts_years(con, data_dir: str = DATA_DIR, sample_files: int = 30) -> tuple:
    """(première, dernière) année couvertes par counts_by_year.
    OpenAlex garde les 10 dernières années (année en cours comprise) :
    première = dernière - 9. On lit la dernière année sur les fichiers les
    plus récemment mis à jour."""
    files = sorted(Path(data_dir, "works").glob("*.parquet"))[-sample_files:]
    if not files:
        raise FileNotFoundError(f"aucun fichier dans {data_dir}/works")
    flist = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    last = con.execute(f"""
        SELECT max(c.year) FROM (
            SELECT unnest(counts_by_year) AS c FROM read_parquet({flist}))
    """).fetchone()[0]
    return int(last) - 9, int(last)


def compute_top10(disciplines: str = DISCIPLINES_NAME,
                  data_dir: str = DATA_DIR,
                  work_types=WORK_TYPES,
                  source_types=SOURCE_TYPES,
                  annee_min: int = ANNEE_MIN,
                  annee_max: int = ANNEE_MAX,
                  citation_window: int = CITATION_WINDOW,
                  top_share: float = TOP_SHARE,
                  exclude_uncited: bool = TOP_EXCLUDE_UNCITED,
                  counts_first_year: int = COUNTS_FIRST_YEAR,
                  world: str = WORLD,
                  out_name: str = None,
                  verbose: bool = True) -> dict:
    """Calcule w_top pour chaque article du périmètre (voir l'en-tête du module).

    disciplines : nom du regroupement (<data_dir>/disciplines/<nom>.parquet)
    world       : "all" (toutes les revues OpenAlex) ou "sjr" (revues SJR seulement)
    out_name    : nom des fichiers de sortie (défaut : <regroupement>, ou
                  <regroupement>_sjr pour le monde SJR)
    Les autres arguments reprennent config.py.

    Renvoie {'fichier', 'n_works', 'counts_years', 'par_annee', 'par_discipline'}.
    """
    data_dir = Path(data_dir)
    out_name = output_name(disciplines, world, out_name)
    sources_file = world_sources(world, data_dir)
    out_dir = data_dir / "top10"
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(data_dir)

    # --- 1. Années couvertes par counts_by_year -------------------------------
    if citation_window is None:
        first, last = None, None
    elif counts_first_year is not None:
        first, last = int(counts_first_year), int(counts_first_year) + 9
    else:
        first, last = detect_counts_years(con, data_dir)

    if citation_window is None:
        cit_expr, mode_expr = "w.cited_by_count", "'cumul'"
    else:
        W = int(citation_window)
        fenetre = (f"coalesce(list_sum(list_transform("
                   f"list_filter(w.counts_by_year, c -> c.year BETWEEN w.year AND w.year + {W}), "
                   f"c -> c.cited_by_count)), 0)")
        cit_expr = f"CASE WHEN w.year < {first} THEN w.cited_by_count ELSE {fenetre} END"
        mode_expr = f"CASE WHEN w.year < {first} THEN 'cumul' ELSE 'fenetre' END"

    # --- 2. Périmètre + citations ----------------------------------------------
    con.execute(f"""
        CREATE TEMP TABLE base AS
        SELECT w.work_id, w.year::SMALLINT AS year, d.discipline_code,
               coalesce({cit_expr}, 0)::BIGINT AS citations,
               w.oa_top10
        FROM read_parquet('{data_dir}/works/*.parquet') w
        JOIN read_parquet('{data_dir}/disciplines/{disciplines}.parquet') d
          ON upper(w.topic_id) = d.topic_id
        WHERE d.discipline_code IS NOT NULL
          AND {perimeter_where(work_types, source_types, annee_min, annee_max, 'w', sources_file)}
    """)

    # --- 3. Poids top 10 % par valeur de citations dans chaque cellule ----------
    excl = "citations = 0 OR " if exclude_uncited else ""
    con.execute(f"""
        CREATE TEMP TABLE poids AS
        WITH cnt AS (
            SELECT discipline_code, year, citations, count(*) AS b
            FROM base GROUP BY ALL
        ), cum AS (
            SELECT *,
                   sum(b) OVER (PARTITION BY discipline_code, year) AS n,
                   coalesce(sum(b) OVER (PARTITION BY discipline_code, year
                                         ORDER BY citations DESC
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS a
            FROM cnt
        )
        SELECT discipline_code, year, citations, n, b,
               CASE WHEN {excl}FALSE THEN 0.0
                    ELSE greatest(0.0, least(1.0, ({top_share} * n - a) / b))
               END::DOUBLE AS w_top
        FROM cum
    """)

    # --- 4. Écritures -----------------------------------------------------------
    f_works = out_dir / f"{out_name}.parquet"
    con.execute(f"""
        COPY (SELECT b.work_id, b.year, b.discipline_code, b.citations, p.w_top
              FROM base b JOIN poids p USING (discipline_code, year, citations))
        TO '{f_works}' (FORMAT parquet, COMPRESSION zstd)
    """)

    prov = (f"(year + {int(citation_window)} >= {last})" if citation_window is not None
            else "FALSE")
    f_seuils = out_dir / f"{out_name}_seuils.parquet"
    con.execute(f"""
        COPY (
            SELECT discipline_code, year,
                   any_value(n)::BIGINT                           AS n,
                   min(citations) FILTER (WHERE w_top > 0)        AS seuil,
                   min_by(w_top, citations) FILTER (WHERE w_top > 0) AS w_seuil,
                   sum(b * w_top) / any_value(n)                  AS part_top,
                   {mode_expr.replace('w.year', 'year')}          AS mode_citations,
                   {prov}                                         AS provisoire
            FROM poids GROUP BY discipline_code, year
            ORDER BY discipline_code, year
        ) TO '{f_seuils}' (FORMAT parquet)
    """)

    params = dict(disciplines=disciplines, world=world, work_types=list(work_types or []),
                  source_types=list(source_types or []), annee_min=annee_min,
                  annee_max=annee_max, citation_window=citation_window,
                  top_share=top_share, exclude_uncited=exclude_uncited,
                  counts_years=[first, last])
    (out_dir / f"{out_name}_params.json").write_text(json.dumps(params, indent=1))

    # --- 5. Contrôles -----------------------------------------------------------
    # concordance : part (pondérée) de notre top que OpenAlex classe aussi top 10 %
    # (OpenAlex : par subfield, citations cumulées -> accord partiel attendu)
    par_annee = con.execute(f"""
        SELECT b.year,
               count(*)                                            AS n,
               round(100 * sum(p.w_top) / count(*), 2)             AS pct_top,
               min(p.citations) FILTER (WHERE p.w_top > 0)         AS seuil_min,
               round(100 * sum(p.w_top * coalesce(b.oa_top10, false)::INT)
                         / nullif(sum(p.w_top), 0), 1)             AS pct_aussi_top_openalex,
               any_value({mode_expr.replace('w.year', 'b.year')})  AS mode_citations,
               any_value({prov.replace('year', 'b.year')})         AS provisoire
        FROM base b JOIN poids p USING (discipline_code, year, citations)
        GROUP BY b.year ORDER BY b.year
    """).df()
    par_discipline = con.execute("""
        SELECT discipline_code, count(*) AS n_works,
               round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct_works
        FROM base GROUP BY 1 ORDER BY 1
    """).df()
    n_works = int(par_annee["n"].sum())
    con.close()

    res = {"fichier": str(f_works), "n_works": n_works, "counts_years": (first, last),
           "par_annee": par_annee, "par_discipline": par_discipline}
    if verbose:
        if first is not None:
            print(f"counts_by_year couvre {first}-{last} : cohortes < {first} en citations cumulées")
        print(f"Périmètre : {n_works:,} articles -> {f_works}\n")
        print(par_annee.to_string(index=False), "\n")
        print(par_discipline.to_string(index=False))
    return res
