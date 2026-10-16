"""
indicators.py
=============
Étape 6 : les indicateurs, par monde x entité x année x discipline.

Entités
-------
  - pays          : la France (PAYS_FR : métropole + outre-mer). Un article
                    compte s'il a au moins un auteur dont le pays est dans
                    PAYS_FR (champ `countries` de l'étape 1).
  - etablissement : chaque ROR d'une institution de ETAB_COUNTRIES. Un article
                    compte pour un ROR si l'une de ses institutions EST ce ROR
                    ou l'a pour tutelle (lignée OpenAlex, étape 2, corrigée par
                    config.LINEAGE_CUTS) : un article d'une UMR compte pour
                    l'UMR, son université et le CNRS. Les ROR de ETAB_EXCLUDE
                    ne sont pas présentés comme établissements.
  Comptage entier : un article compte 1 pour chaque entité présente, quel que
  soit le nombre de ses auteurs ou de ses autres pays.

Indicateurs (pour une entité E, une année, une discipline d, un monde)
---------------------------------------------------------------------
  n_monde, n_ent      articles du monde / de E
  s = n_ent / n_monde                                    part de E dans le monde
  INFLUENCE
  top_monde, top_ent  somme des poids w_top (étape 4) ; top_monde = 10 % de n_monde
  x = top_ent / top_monde        part de E dans le top 10 % mondial des plus cités
  indice_x = x / s               > 1 : E est sur-représentée dans le top 10 %
  pp_top = top_ent / n_ent       part des articles de E qui sont dans le top 10 %
  PRESTIGE
  tj_monde, tj_ent    articles de d parus dans une revue de tête de d (étape 5,
                      revues disciplinaires + multidisciplinaires)
  y = tj_ent / tj_monde          part de E dans les articles des revues de tête
  indice_y = y / s
  tj_monde_disc, tj_ent_disc, y_disc : idem SANS les revues multidisciplinaires
                      (pour mesurer leur effet)
  provisoire          fenêtre de citation pas encore close (x moins stable)

  Les numérateurs et dénominateurs sont conservés : un article n'a qu'une
  discipline et qu'une année, donc on peut SOMMER les comptes sur plusieurs
  années ou disciplines puis recalculer les ratios (ne jamais moyenner x, y, s).
  Les lignes discipline_code = 'ALL' donnent déjà le total toutes disciplines.

  Remarque : les revues de tête sont toutes dans SJR, donc tj_monde et y sont
  identiques dans les deux mondes ; seuls n_monde, s et x changent.

Sorties (dans <data_dir>/indicators/)
-------------------------------------
  <nom>.parquet                une ligne par monde x entité x année x discipline
  <nom>_etablissements.parquet ror, nom, pays, type, n_articles (monde "all") :
                               liste pour le filtre du dashboard
  <nom>_params.json            paramètres utilisés

Exemple :
    from project.server.main.indicators import compute_indicators
    res = compute_indicators()
    res["france"]          # France, toutes disciplines, par année et par monde
"""

import json
from pathlib import Path

import pandas as pd

from .common import PAYS_NOMS, connect, output_name, pays_list_sql
from .config import (ANNEE_MAX, ANNEE_MIN, DATA_DIR, DISCIPLINES_NAME, ETAB_COUNTRIES,
                     ETAB_EXCLUDE, ETAB_INTERNATIONAL_N, ETAB_INTERNATIONAL_RORS,
                     ETAB_INTERNATIONAL_TYPES, ETAB_MIN_ARTICLES, INDICATOR_WORLDS, LINEAGE_CUTS,
                     PAYS_COMPARAISON, PAYS_COMPARAISON_N, PAYS_FR)
from .lineage_check import apply_lineage_cuts


def _top10_file(data_dir: Path, disciplines: str, world: str) -> tuple:
    name = output_name(disciplines, world)
    f = data_dir / "top10" / f"{name}.parquet"
    if not f.exists():
        raise FileNotFoundError(f"{f} absent : lancer compute_top10(disciplines={disciplines!r}, "
                                f"world={world!r})")
    p = data_dir / "top10" / f"{name}_params.json"
    params = json.loads(p.read_text()) if p.exists() else {}
    return f, params


def _provisoire_sql(params: dict) -> str:
    W, years = params.get("citation_window"), params.get("counts_years") or [None, None]
    if W is None or years[1] is None:
        return "FALSE"
    return f"(e.year + {int(W)} >= {int(years[1])})"


def compute_indicators(disciplines: str = DISCIPLINES_NAME,
                       worlds=INDICATOR_WORLDS,
                       top_journals_name: str = None,
                       data_dir: str = DATA_DIR,
                       pays=PAYS_FR,
                       etab_countries=ETAB_COUNTRIES,
                       etab_min_articles: int = ETAB_MIN_ARTICLES,
                       lineage_cuts=LINEAGE_CUTS,
                       etab_exclude=ETAB_EXCLUDE,
                       pays_comparaison=PAYS_COMPARAISON,
                       pays_comparaison_n: int = PAYS_COMPARAISON_N,
                       etab_international_rors=ETAB_INTERNATIONAL_RORS,
                       etab_international_n: int = ETAB_INTERNATIONAL_N,
                       etab_international_types=ETAB_INTERNATIONAL_TYPES,
                       annee_min: int = ANNEE_MIN,
                       annee_max: int = ANNEE_MAX,
                       out_name: str = None,
                       verbose: bool = True) -> dict:
    """Calcule les indicateurs (voir l'en-tête du module).

    disciplines       : regroupement (étapes 3 à 5)
    worlds            : mondes à calculer ; chacun exige /data/top10/<fichier>
    top_journals_name : liste de revues de tête (défaut : nom du regroupement)
    etab_countries    : None = pas d'établissements (France seulement)
    lineage_cuts      : corrections de la lignée OpenAlex (voir config.LINEAGE_CUTS)
    etab_exclude      : ROR à ne pas présenter comme établissements
    pays_comparaison  : pays comparés à la France (None = les pays_comparaison_n plus gros)
    etab_international_rors / _n / _types : établissements étrangers de comparaison
                        (liste imposée, ou les N plus gros des types indiqués)
    out_name          : défaut : nom du regroupement

    Renvoie {'fichier', 'n_lignes', 'n_etablissements', 'france', 'top_etablissements'}.
    """
    data_dir = Path(data_dir)
    out_name = out_name or disciplines
    tj_name = top_journals_name or disciplines
    out_dir = data_dir / "indicators"
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(data_dir)
    fr = pays_list_sql(pays)

    f_tj = data_dir / "top_journals" / f"{tj_name}.parquet"
    f_ss = data_dir / "top_journals" / "sjr_sources.parquet"
    for f in (f_tj, f_ss):
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : lancer compute_top_journals()")

    # Revues de tête : (discipline, revue SJR) ; disc = 1 si revue disciplinaire
    con.execute(f"""
        CREATE TEMP TABLE tj AS
        SELECT discipline_code, sjr_id, max((categorie = 'disciplinaire')::INT) AS disc
        FROM read_parquet('{f_tj}') GROUP BY ALL
    """)

    # Établissements cibles : institution -> ROR (elle-même ou une tutelle)
    if etab_countries:
        apply_lineage_cuts(con, data_dir, lineage_cuts, table="lin_ok")
        excl = pays_list_sql([r.split("/")[-1] for r in (etab_exclude or [])]) or "NULL"
        con.execute(f"""
            CREATE TEMP TABLE inst_all AS
            SELECT inst_id, ror, country_code, type FROM read_parquet('{data_dir}/institutions.parquet')
        """)
        # Établissements étrangers de comparaison : liste imposée, ou les N plus gros
        # (taille approchée = somme des articles affiliés directement à l'établissement
        #  et à ses structures rattachées ; sert seulement à les choisir)
        if etab_international_rors:
            rors = pays_list_sql([r.split("/")[-1] for r in etab_international_rors])
            con.execute(f"CREATE TEMP TABLE etranger AS SELECT ror FROM inst_all WHERE ror IN ({rors})")
        elif etab_international_n:
            types = (f"AND a.type IN ({pays_list_sql(etab_international_types)})"
                     if etab_international_types else "")
            con.execute(f"""
                CREATE TEMP TABLE etranger AS
                WITH nd AS (
                    SELECT inst_id, count(DISTINCT work_id) AS n
                    FROM read_parquet('{data_dir}/work_inst/*.parquet') GROUP BY 1
                )
                SELECT a.ror, sum(nd.n) AS taille
                FROM lin_ok l
                JOIN nd ON nd.inst_id = l.inst_id
                JOIN inst_all a ON a.inst_id = l.anc_inst_id
                WHERE a.ror IS NOT NULL AND a.ror <> ''
                  AND a.country_code NOT IN ({pays_list_sql(etab_countries)}) {types}
                GROUP BY a.ror ORDER BY taille DESC LIMIT {int(etab_international_n)}
            """)
        else:
            con.execute("CREATE TEMP TABLE etranger AS SELECT NULL::VARCHAR AS ror WHERE FALSE")
        con.execute(f"""
            CREATE TEMP TABLE cible AS
            SELECT DISTINCT l.inst_id, l.anc_ror AS ror
            FROM lin_ok l
            JOIN inst_all i ON i.inst_id = l.anc_inst_id
            WHERE l.anc_ror IS NOT NULL AND l.anc_ror <> ''
              AND (i.country_code IN ({pays_list_sql(etab_countries)})
                   OR l.anc_ror IN (SELECT ror FROM etranger))
              AND l.anc_ror NOT IN ({excl})
        """)

    lignes, etab_keep = [], None
    for world in worlds:
        f_top10, params10 = _top10_file(data_dir, disciplines, world)
        if verbose:
            print(f"[{world}] {f_top10.name}")

        # Articles du monde : discipline, poids top, France, revue de tête
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE base AS
            SELECT t.work_id, t.year, t.discipline_code, t.w_top,
                   -- pays de l'article, codes d'outre-mer regroupés sous FR
                   list_distinct(list_transform(coalesce(w.countries, []),
                       c -> CASE WHEN c IN ({fr}) THEN 'FR' ELSE c END)) AS pays,
                   (tj.sjr_id IS NOT NULL)               AS in_tj,
                   coalesce(tj.disc = 1, false)          AS in_tj_disc
            FROM read_parquet('{f_top10}') t
            JOIN (SELECT work_id, countries, source_id
                  FROM read_parquet('{data_dir}/works/*.parquet')) w USING (work_id)
            LEFT JOIN read_parquet('{f_ss}') ss ON ss.source_id = w.source_id
            LEFT JOIN tj ON tj.discipline_code = t.discipline_code AND tj.sjr_id = ss.sjr_id
            WHERE t.year BETWEEN {int(annee_min)} AND {int(annee_max)}
        """)

        agg = """count(*)::BIGINT AS n, sum(w_top)::DOUBLE AS top,
                 sum(in_tj::INT)::BIGINT AS tj, sum(in_tj_disc::INT)::BIGINT AS tj_disc"""
        sets = "GROUPING SETS ((year, discipline_code), (year))"

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE monde AS
            SELECT year, coalesce(discipline_code, 'ALL') AS discipline_code, {agg}
            FROM base GROUP BY {sets}
        """)
        if not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'pays_keep'").fetchone()[0]:
            # pays comparés, fixés sur le premier monde (le plus large) ; la France toujours
            if pays_comparaison:
                liste = sorted({"FR", *[p.upper() for p in pays_comparaison]})
                con.execute("CREATE TEMP TABLE pays_keep AS SELECT unnest(?::VARCHAR[]) AS pays", [liste])
            else:
                con.execute(f"""
                    CREATE TEMP TABLE pays_keep AS
                    SELECT pays FROM (SELECT unnest(pays) AS pays FROM base)
                    GROUP BY pays ORDER BY count(*) DESC LIMIT {int(pays_comparaison_n)}
                """)
                con.execute("INSERT INTO pays_keep SELECT 'FR' WHERE 'FR' NOT IN (SELECT pays FROM pays_keep)")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ent AS
            SELECT 'pays' AS entity_type, b.p AS entity_id,
                   year, coalesce(discipline_code, 'ALL') AS discipline_code, {agg}
            FROM (SELECT unnest(pays) AS p, year, discipline_code, w_top, in_tj, in_tj_disc
                  FROM base) b
            JOIN pays_keep k ON k.pays = b.p
            GROUP BY GROUPING SETS ((b.p, year, discipline_code), (b.p, year))
        """)

        if etab_countries:
            con.execute("""
                CREATE OR REPLACE TEMP TABLE work_ror AS
                SELECT DISTINCT wi.work_id, c.ror
                FROM read_parquet('{d}/work_inst/*.parquet') wi
                JOIN cible c USING (inst_id)
                WHERE wi.work_id IN (SELECT work_id FROM base)
            """.format(d=data_dir))
            if etab_keep is None:
                # liste des établissements fixée sur le premier monde (le plus large)
                con.execute(f"""
                    CREATE TEMP TABLE etab_keep AS
                    SELECT ror, count(*) AS n_articles FROM work_ror
                    GROUP BY ror HAVING count(*) >= {int(etab_min_articles)}
                """)
                etab_keep = True
            con.execute(f"""
                INSERT INTO ent
                SELECT 'etablissement', wr.ror, b.year,
                       coalesce(b.discipline_code, 'ALL'), {agg}
                FROM work_ror wr
                JOIN etab_keep k USING (ror)
                JOIN base b USING (work_id)
                GROUP BY GROUPING SETS ((wr.ror, b.year, b.discipline_code), (wr.ror, b.year))
            """)

        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE res_{world} AS
            SELECT '{world}' AS world, e.entity_type, e.entity_id, e.year, e.discipline_code,
                   m.n AS n_monde, m.top AS top_monde, m.tj AS tj_monde, m.tj_disc AS tj_monde_disc,
                   e.n AS n_ent, e.top AS top_ent, e.tj AS tj_ent, e.tj_disc AS tj_ent_disc,
                   e.n / m.n                              AS s,
                   e.top / nullif(m.top, 0)               AS x,
                   e.tj / nullif(m.tj, 0)                 AS y,
                   e.tj_disc / nullif(m.tj_disc, 0)       AS y_disc,
                   (e.top / nullif(m.top, 0)) / (e.n / m.n) AS indice_x,
                   (e.tj / nullif(m.tj, 0)) / (e.n / m.n)   AS indice_y,
                   e.top / e.n                            AS pp_top,
                   e.tj / e.n                             AS pp_tj,
                   {_provisoire_sql(params10)}            AS provisoire
            FROM ent e JOIN monde m USING (year, discipline_code)
        """)
        lignes.append(f"res_{world}")

    # Noms des entités et des disciplines, écriture
    union = " UNION ALL ".join(f"SELECT * FROM {t}" for t in lignes)
    con.execute(f"""
        CREATE TEMP TABLE noms_etab AS
        SELECT ror, any_value(display_name) AS nom, any_value(country_code) AS pays,
               any_value(type) AS type
        FROM read_parquet('{data_dir}/institutions.parquet')
        WHERE ror IS NOT NULL AND ror <> '' GROUP BY ror
    """)
    con.register("pays_noms", pd.DataFrame(list(PAYS_NOMS.items()), columns=["code", "nom"]))
    f_out = out_dir / f"{out_name}.parquet"
    con.execute(f"""
        COPY (
            SELECT r.world, r.entity_type, r.entity_id,
                   CASE WHEN r.entity_type = 'pays' THEN coalesce(pn.nom, r.entity_id) ELSE ne.nom END
                       AS entity_name,
                   r.year, r.discipline_code,
                   CASE WHEN r.discipline_code = 'ALL' THEN 'Toutes disciplines'
                        ELSE dn.discipline_name END AS discipline_name,
                   r.* EXCLUDE (world, entity_type, entity_id, year, discipline_code)
            FROM ({union}) r
            LEFT JOIN noms_etab ne ON r.entity_type = 'etablissement' AND ne.ror = r.entity_id
            LEFT JOIN pays_noms pn ON r.entity_type = 'pays' AND pn.code = r.entity_id
            LEFT JOIN (SELECT DISTINCT discipline_code, discipline_name
                       FROM read_parquet('{data_dir}/disciplines/{disciplines}.parquet')) dn
                   USING (discipline_code)
            ORDER BY world, entity_type DESC, entity_id, year, discipline_code
        ) TO '{f_out}' (FORMAT parquet, COMPRESSION zstd)
    """)

    n_etab = 0
    if etab_countries:
        con.execute(f"""
            COPY (SELECT k.ror, ne.nom, ne.pays, ne.type, k.n_articles
                  FROM etab_keep k LEFT JOIN noms_etab ne USING (ror)
                  ORDER BY k.n_articles DESC)
            TO '{out_dir}/{out_name}_etablissements.parquet' (FORMAT parquet)
        """)
        n_etab = con.execute("SELECT count(*) FROM etab_keep").fetchone()[0]

    params = dict(disciplines=disciplines, worlds=list(worlds), top_journals=tj_name,
                  pays=list(pays), etab_countries=list(etab_countries or []),
                  etab_min_articles=etab_min_articles,
                  lineage_cuts=[list(c) for c in (lineage_cuts or [])],
                  etab_exclude=list(etab_exclude or []), annee_min=annee_min, annee_max=annee_max,
                  pays_compares=[r[0] for r in con.execute("SELECT pays FROM pays_keep ORDER BY 1").fetchall()])
    (out_dir / f"{out_name}_params.json").write_text(json.dumps(params, indent=1))

    # Contrôles ---------------------------------------------------------------
    q = f"read_parquet('{f_out}')"
    res = {
        "fichier": str(f_out),
        "n_lignes": con.execute(f"SELECT count(*) FROM {q}").fetchone()[0],
        "n_etablissements": n_etab,
        "france": con.execute(f"""
            SELECT world, year, n_monde, n_ent,
                   round(100 * s, 2) AS s_pct, round(100 * x, 2) AS x_pct,
                   round(100 * y, 2) AS y_pct, round(100 * y_disc, 2) AS y_disc_pct,
                   round(indice_x, 2) AS indice_x, round(indice_y, 2) AS indice_y,
                   round(100 * top_monde / n_monde, 2) AS controle_top_pct, provisoire
            FROM {q} WHERE entity_id = 'FR' AND discipline_code = 'ALL'
            ORDER BY world, year
        """).df(),
        "top_etablissements": con.execute(f"""
            SELECT entity_id AS ror, any_value(entity_name) AS nom, sum(n_ent)::BIGINT AS n,
                   round(100 * sum(top_ent) / sum(top_monde), 3) AS x_pct,
                   round(100 * sum(tj_ent) / sum(tj_monde), 3) AS y_pct,
                   round((sum(top_ent) / sum(n_ent)) / (sum(top_monde) / sum(n_monde)), 2)
                       AS indice_x
            FROM {q}
            WHERE entity_type = 'etablissement' AND discipline_code = 'ALL' AND world = 'all'
              AND entity_id IN (SELECT ror FROM noms_etab WHERE pays = 'FR')
            GROUP BY 1 ORDER BY n DESC LIMIT 15
        """).df() if n_etab else None,
        "pays": con.execute(f"""
            SELECT entity_id AS pays, any_value(entity_name) AS nom,
                   round(100 * sum(top_ent) FILTER (WHERE year = {int(annee_min)})
                         / sum(top_monde) FILTER (WHERE year = {int(annee_min)}), 2) AS x_debut_pct,
                   round(100 * sum(top_ent) FILTER (WHERE year = {int(annee_max)})
                         / sum(top_monde) FILTER (WHERE year = {int(annee_max)}), 2) AS x_fin_pct
            FROM {q} WHERE entity_type = 'pays' AND discipline_code = 'ALL' AND world = '{worlds[-1]}'
            GROUP BY 1 ORDER BY x_fin_pct DESC
        """).df(),
        "n_etrangers": con.execute(f"""
            SELECT count(DISTINCT entity_id) FROM {q} WHERE entity_type = 'etablissement'
              AND entity_id NOT IN (SELECT ror FROM noms_etab
                                    WHERE pays IN ({pays_list_sql(etab_countries or ['FR'])}))
        """).fetchone()[0] if n_etab else 0,
    }
    con.close()

    if verbose:
        print(f"\n{res['n_lignes']:,} lignes -> {f_out} ({n_etab:,} établissements, "
              f"dont {res['n_etrangers']:,} étrangers ; {len(res['pays'])} pays)")
        print("\nFrance, toutes disciplines (en %) :")
        print(res["france"].to_string(index=False))
        if res["top_etablissements"] is not None:
            print("\n15 plus gros établissements, monde 'all', toute la période (en %) :")
            print(res["top_etablissements"].to_string(index=False))
        print(f"\nPays comparés, part du top 10 % (monde '{worlds[-1]}', toutes disciplines, en %) :")
        print(res["pays"].to_string(index=False))
    return res
