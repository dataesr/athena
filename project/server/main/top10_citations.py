"""
top10_citations.py
==================
Étape 4 : repérer les articles du top 10 % des plus cités de leur discipline
et de leur année.

Méthode
-------
1. Périmètre de référence (« le monde ») : articles extraits filtrés par
   config.py (WORK_TYPES, SOURCE_TYPES, ANNEE_MIN, ANNEE_MAX, PERIMETRE),
   avec une discipline.
2. Citations de chaque article :
   - CITATION_WINDOW = N : citations reçues de l'année de publication à N ans
     après, tirées de counts_by_year. OpenAlex ne garde que ~10 ans dans
     counts_by_year : une cohorte dont la fenêtre commence avant la première
     année couverte bascule sur cited_by_count cumulé. Comme le classement se
     fait à l'intérieur d'une année, ce changement de mesure ne biaise pas
     la comparaison entre articles d'une même cohorte.
   - CITATION_WINDOW = None : cited_by_count cumulé pour toutes les années.
3. Dans chaque cellule discipline x année (N articles), on classe par citations
   décroissantes. Pour un article à c citations, avec A articles strictement
   plus cités et B articles ex aequo (B compte l'article lui-même) :
        fraction_top10_cites = min(1, max(0, (TOP_SHARE * N - A) / B))
   -> les articles nettement au-dessus du seuil valent 1, ceux du seuil
      se partagent la place restante, et le total vaut exactement 10 % de n
      (méthode de Waltman & Schreiber, 2013).
   Avec TOP_EXCLUDE_UNCITED, un article jamais cité vaut toujours 0.

Sorties (dans <data_dir>/top10/, <nom> = <classement>_<perimetre>)
-------------------------------------------------------------------
  <nom>.parquet          une ligne par article du périmètre
      work_id, annee_publication, discipline_code, nb_citations, fraction_top10_cites
      (fraction_top10_cites : 1 = entièrement dans le top, 0 = hors du top,
       entre les deux = ex aequo au seuil ; la somme d'une cellule vaut 10 % de N)
  <nom>_seuils.parquet   une ligne par discipline x année
      discipline_code, annee_publication, nb_articles, nb_articles_top10_cites,
      seuil_nb_citations (citations minimales pour entrer dans le top),
      nb_articles_au_seuil (ex aequo au seuil), fraction_au_seuil,
      mode_citations ('fenetre' ou 'cumul'), influence_provisoire
  <nom>_params.json      paramètres utilisés (traçabilité)

Exemple :
    from project.server.main.top10_citations import compute_top10
    res = compute_top10()                                   # paramètres de config.py
    res = compute_top10(classement="erc_alt")               # autre classement
    res = compute_top10(perimetre="revues_sjr")             # -> erc_revues_sjr.parquet
    res = compute_top10(citation_window=None, out_name="erc_cumul")
"""

import json
from pathlib import Path

from .common import connect, nom_fichier, perimeter_where, perimetre_sources
from .config import (ANNEE_MAX, ANNEE_MIN, CITATION_WINDOW, CLASSEMENT, COUNTS_FIRST_YEAR,
                     DATA_DIR, PERIMETRE, SOURCE_TYPES, TOP_EXCLUDE_UNCITED,
                     TOP_SHARE, WORK_TYPES)


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


def compute_top10(classement: str = CLASSEMENT,
                  data_dir: str = DATA_DIR,
                  work_types=WORK_TYPES,
                  source_types=SOURCE_TYPES,
                  annee_min: int = ANNEE_MIN,
                  annee_max: int = ANNEE_MAX,
                  citation_window: int = CITATION_WINDOW,
                  top_share: float = TOP_SHARE,
                  exclude_uncited: bool = TOP_EXCLUDE_UNCITED,
                  counts_first_year: int = COUNTS_FIRST_YEAR,
                  perimetre: str = PERIMETRE,
                  out_name: str = None,
                  verbose: bool = True) -> dict:
    """Calcule fraction_top10_cites pour chaque article (voir l'en-tête du module).

    classement : <data_dir>/disciplines/<classement>.parquet
    perimetre  : "toutes_revues_openalex" ou "revues_sjr"
    out_name   : nom des fichiers de sortie (défaut : <classement>_<perimetre>)
    Les autres arguments reprennent config.py.

    Renvoie {'fichier', 'nb_articles', 'counts_years', 'par_annee', 'par_discipline'}.
    """
    data_dir = Path(data_dir)
    out_name = out_name or nom_fichier(classement, perimetre)
    sources_file = perimetre_sources(perimetre, data_dir)
    out_dir = data_dir / "top10"
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(data_dir)

    # --- 1. Années couvertes par counts_by_year -------------------------------
    # counts_by_year ne garde que les 10 dernières années de citations (ex. 2017-2026
    # pour un snapshot de 2026). Une fenêtre « année + 3 ans » n'est complète que si
    # elle commence après la première année couverte : sinon (cohortes 2015, 2016),
    # on prend les citations cumulées (cited_by_count). Le classement se faisant à
    # l'intérieur d'une même année, chaque cohorte est classée avec une seule mesure.
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
        # Citations dans la fenêtre : on filtre counts_by_year sur les années
        # [année de publication, année + W], puis on additionne les citations.
        # Exemple : article de 2019, W = 3, counts_by_year =
        #   [{2019: 2}, {2020: 10}, {2021: 7}, {2022: 4}, {2023: 9}]
        #   list_filter (2019 <= année <= 2022) -> [{2019: 2}, {2020: 10}, {2021: 7}, {2022: 4}]
        #   list_transform (garde le nombre)   -> [2, 10, 7, 4] ; list_sum -> 23
        #   (coalesce 0 : un article sans aucune citation a une liste vide -> NULL -> 0)
        fenetre = (f"coalesce(list_sum(list_transform("
                   f"list_filter(w.counts_by_year, c -> c.year BETWEEN w.year AND w.year + {W}), "
                   f"c -> c.cited_by_count)), 0)")
        cit_expr = f"CASE WHEN w.year < {first} THEN w.cited_by_count ELSE {fenetre} END"
        mode_expr = f"CASE WHEN w.year < {first} THEN 'cumul' ELSE 'fenetre' END"

    # --- 2. Périmètre + citations ----------------------------------------------
    # Une ligne par article du périmètre : discipline (jointure topic -> discipline
    # de l'étape 3) et nombre de citations (fenêtre ou cumul, voir ci-dessus).
    # upper(topic_id) : les topics sont écrits 'T10001' dans le fichier de classement.
    # perimeter_where : types de documents, types de sources, années, et pour le
    # périmètre revues_sjr, seulement les sources appariées à SJR.
    con.execute(f"""
        CREATE TEMP TABLE base AS
        SELECT w.work_id, w.year::SMALLINT AS annee_publication, d.discipline_code,
               coalesce({cit_expr}, 0)::BIGINT AS nb_citations,
               w.oa_top10
        FROM read_parquet('{data_dir}/works/*.parquet') w
        JOIN read_parquet('{data_dir}/disciplines/{classement}.parquet') d
          ON upper(w.topic_id) = d.topic_id
        WHERE d.discipline_code IS NOT NULL
          AND {perimeter_where(work_types, source_types, annee_min, annee_max, 'w', sources_file)}
    """)

    # --- 3. Fraction dans le top 10 %, par valeur de citations et par cellule ------
    # Cellule = discipline x année. On ne travaille pas article par article mais
    # par « paquet » d'articles ayant le même nombre de citations dans la cellule.
    #
    # Avec exclude_uncited, un article sans citation ne peut pas entrer dans le top
    # (sa fraction vaut 0, quelle que soit la place restante).
    # Le CTE par_valeur regroupe les articles par discipline, année et nombre de
    #   citations : nb_ex_aequo est la taille du paquet.
    # Le CTE cumul calcule nb_articles_cellule = le nombre total d'articles de la
    #   cellule (somme des paquets).
    # Le CTE cumul calcule aussi nb_plus_cites = le nombre d'articles STRICTEMENT plus
    #   cités que le paquet courant : tri par citations décroissantes, puis somme des
    #   nb_ex_aequo de toutes les lignes précédentes en excluant la ligne courante
    #   (ROWS ... AND 1 PRECEDING). Pour le paquet le plus cité, il n'y a rien avant :
    #   la somme vaut NULL, remplacée par 0 par le coalesce.
    # fraction_top10_cites : (top_share * nb_articles_cellule) = le nombre de places
    #   dans le top, MOINS nb_plus_cites, donne le nombre de places encore libres une
    #   fois servis tous les articles plus cités. En divisant par nb_ex_aequo, on
    #   partage ces places à égalité entre les articles du paquet. least(1, ...) :
    #   un article ne vaut jamais plus de 1 ; greatest(0, ...) : quand le top est
    #   déjà rempli, la fraction vaut 0.
    #
    # Exemple : cellule de 100 articles, top_share = 0,1 -> 10 places dans le top
    #
    #   nb_citations | nb_ex_aequo | nb_plus_cites | (10 - plus_cites) / ex_aequo | fraction
    #   -------------+-------------+---------------+------------------------------+---------
    #             50 |           3 |             0 |                         3,33 |  1   (tout le paquet tient dans le top)
    #             40 |           4 |             3 |                         1,75 |  1   (tout le paquet tient dans le top)
    #             30 |           6 |             7 |                         0,50 |  0,5 (3 places restantes pour 6 ex aequo)
    #             20 |          10 |            13 |                         < 0  |  0   (top déjà rempli)
    #
    #   Vérification : 3 x 1 + 4 x 1 + 6 x 0,5 = 10 = top_share x nb_articles_cellule.
    #   C'est la méthode de Waltman et Schreiber (2013) : le top contient exactement
    #   10 % des articles, d'où des « équivalent-articles » décimaux.
    excl = "nb_citations = 0 OR " if exclude_uncited else ""
    con.execute(f"""
        CREATE TEMP TABLE fractions AS
        WITH par_valeur AS (
            SELECT discipline_code, annee_publication, nb_citations, count(*) AS nb_ex_aequo
            FROM base GROUP BY ALL
        ), cumul AS (
            SELECT *,
                   sum(nb_ex_aequo) OVER (PARTITION BY discipline_code, annee_publication)
                       AS nb_articles_cellule,
                   coalesce(sum(nb_ex_aequo) OVER (PARTITION BY discipline_code, annee_publication
                                                  ORDER BY nb_citations DESC
                                                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
                       AS nb_plus_cites
            FROM par_valeur
        )
        SELECT discipline_code, annee_publication, nb_citations, nb_articles_cellule, nb_ex_aequo,
               CASE WHEN {excl}FALSE THEN 0.0
                    ELSE greatest(0.0, least(1.0,
                         ({top_share} * nb_articles_cellule - nb_plus_cites) / nb_ex_aequo))
               END::DOUBLE AS fraction_top10_cites
        FROM cumul
    """)

    # --- 4. Écritures -----------------------------------------------------------
    # Chaque article reçoit la fraction de son paquet : jointure sur la clé du
    # paquet (discipline, année, nombre de citations).
    f_works = out_dir / f"{out_name}.parquet"
    con.execute(f"""
        COPY (SELECT b.work_id, b.annee_publication, b.discipline_code, b.nb_citations,
                     f.fraction_top10_cites
              FROM base b JOIN fractions f USING (discipline_code, annee_publication, nb_citations))
        TO '{f_works}' (FORMAT parquet, COMPRESSION zstd)
    """)

    prov = (f"(annee_publication + {int(citation_window)} >= {last})" if citation_window is not None
            else "FALSE")
    mode_annee = mode_expr.replace("w.year", "annee_publication")
    f_seuils = out_dir / f"{out_name}_seuils.parquet"
    # Une ligne par cellule, pour vérifier la méthode :
    #   nb_articles_top10_cites = somme(nb_ex_aequo x fraction) : doit valoir 10 % de nb_articles
    #     (un peu moins si plus de 90 % des articles de la cellule ne sont pas cités) ;
    #   seuil_nb_citations : nombre de citations du paquet le MOINS cité qui entre
    #     (même partiellement) dans le top = min(nb_citations) parmi les fractions > 0 ;
    #   nb_articles_au_seuil, fraction_au_seuil : taille et fraction de ce paquet
    #     (min_by(x, nb_citations) = valeur de x sur la ligne au nombre de citations minimal).
    #   Avec l'exemple ci-dessus : seuil 30 citations, 6 articles au seuil, fraction 0,5.
    con.execute(f"""
        COPY (
            SELECT discipline_code, annee_publication,
                   any_value(nb_articles_cellule)::BIGINT                       AS nb_articles,
                   sum(nb_ex_aequo * fraction_top10_cites)                      AS nb_articles_top10_cites,
                   min(nb_citations) FILTER (WHERE fraction_top10_cites > 0)    AS seuil_nb_citations,
                   min_by(nb_ex_aequo, nb_citations) FILTER (WHERE fraction_top10_cites > 0)
                                                                                AS nb_articles_au_seuil,
                   min_by(fraction_top10_cites, nb_citations) FILTER (WHERE fraction_top10_cites > 0)
                                                                                AS fraction_au_seuil,
                   {mode_annee}                                                 AS mode_citations,
                   {prov}                                                       AS influence_provisoire
            FROM fractions GROUP BY discipline_code, annee_publication
            ORDER BY discipline_code, annee_publication
        ) TO '{f_seuils}' (FORMAT parquet)
    """)

    params = dict(classement=classement, perimetre=perimetre, work_types=list(work_types or []),
                  source_types=list(source_types or []), annee_min=annee_min,
                  annee_max=annee_max, citation_window=citation_window,
                  top_share=top_share, exclude_uncited=exclude_uncited,
                  counts_years=[first, last])
    (out_dir / f"{out_name}_params.json").write_text(json.dumps(params, indent=1))

    # --- 5. Contrôles -----------------------------------------------------------
    # Par année : pct_top10_cites doit valoir 10 (%).
    # pct_aussi_top10_openalex : part de NOTRE top (pondérée par les fractions) que
    # OpenAlex classe aussi dans son top 10 % (citation_normalized_percentile).
    # Accord partiel attendu : OpenAlex classe par subfield et en citations cumulées.
    par_annee = con.execute(f"""
        SELECT b.annee_publication,
               count(*)                                                       AS nb_articles,
               round(100 * sum(f.fraction_top10_cites) / count(*), 2)         AS pct_top10_cites,
               min(f.nb_citations) FILTER (WHERE f.fraction_top10_cites > 0)  AS seuil_min_nb_citations,
               round(100 * sum(f.fraction_top10_cites * coalesce(b.oa_top10, false)::INT)
                         / nullif(sum(f.fraction_top10_cites), 0), 1)         AS pct_aussi_top10_openalex,
               any_value({mode_expr.replace('w.year', 'b.annee_publication')}) AS mode_citations,
               any_value({prov.replace('annee_publication', 'b.annee_publication')})
                                                                              AS influence_provisoire
        FROM base b JOIN fractions f USING (discipline_code, annee_publication, nb_citations)
        GROUP BY b.annee_publication ORDER BY b.annee_publication
    """).df()
    par_discipline = con.execute("""
        SELECT discipline_code, count(*) AS nb_articles,
               round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct_articles
        FROM base GROUP BY 1 ORDER BY 1
    """).df()
    nb_articles = int(par_annee["nb_articles"].sum())
    con.close()

    res = {"fichier": str(f_works), "nb_articles": nb_articles, "counts_years": (first, last),
           "par_annee": par_annee, "par_discipline": par_discipline}
    if verbose:
        if first is not None:
            print(f"counts_by_year couvre {first}-{last} : cohortes < {first} en citations cumulées")
        print(f"Périmètre {perimetre} : {nb_articles:,} articles -> {f_works}\n")
        print(par_annee.to_string(index=False), "\n")
        print(par_discipline.to_string(index=False))
    return res
