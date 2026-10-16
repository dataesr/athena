"""
lineage_check.py
================
Diagnostic des rattachements établissement -> tutelles (lignée OpenAlex).

À l'étape 6, un article compte pour un ROR si l'une de ses institutions EST
ce ROR ou l'a dans sa lignée. Si la lignée OpenAlex contient une erreur
(ex. une grande université rattachée à tort à une petite), la petite
« hérite » de tous les articles de la grande.

Outils :
  suspicious_ancestors()  repère les ROR dont le volume vient presque
                          entièrement d'un descendant plus gros qu'eux
  lineage_report(ror)     détaille un ROR : ses ancêtres, et les
                          institutions qui lui apportent le plus d'articles
  preview_lineage_cuts()  effet des corrections LINEAGE_CUTS (config.py) :
                          tutelles retirées, institutions et articles concernés
  apply_lineage_cuts()    utilisé par l'étape 6 : lignée corrigée

Correction d'un rattachement erroné (config.LINEAGE_CUTS) :
  (C, A) = « C n'est pas rattaché à A ». La lignée OpenAlex étant déjà
  « dépliée » (chaque institution liste toutes ses tutelles, directes ou non),
  on ne connaît pas le chemin exact : pour toute institution X qui a C dans sa
  lignée, on retire A et toutes les tutelles de A. Si l'une de ces tutelles
  était aussi une vraie tutelle de X par un autre chemin, elle serait retirée
  à tort : preview_lineage_cuts() liste précisément ce qui est retiré.

Exemple :
    from project.server.main.lineage_check import suspicious_ancestors, lineage_report
    suspicious_ancestors()
    lineage_report("04wez5e68")     # Université Paris 8
"""

from pathlib import Path

from .common import connect, pays_list_sql
from .config import DATA_DIR, LINEAGE_CUTS, PAYS_FR


def _prepare(con, data_dir: Path) -> None:
    """Articles par institution (tous les articles extraits) et lignée."""
    con.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS inst AS
        SELECT * FROM read_parquet('{data_dir}/institutions.parquet')
    """)
    con.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS lin AS
        SELECT * FROM read_parquet('{data_dir}/inst_lineage.parquet')
    """)
    con.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS n_inst AS
        SELECT inst_id, count(DISTINCT work_id) AS n_direct
        FROM read_parquet('{data_dir}/work_inst/*.parquet') GROUP BY 1
    """)


def suspicious_ancestors(data_dir: str = DATA_DIR, pays=PAYS_FR, min_articles: int = 1000,
                         top: int = 30):
    """ROR dont le plus gros contributeur est un descendant PLUS GROS qu'eux.

    Pour chaque ancêtre A (pays de `pays`) :
      n_direct_A  articles affiliés directement à A
      n_lignee_A  articles affiliés à A ou à un descendant (approximation :
                  somme sur les descendants, sans dédoublonnage)
      plus_gros_descendant / son n_direct
    Une tutelle normale (CNRS, une université) reçoit beaucoup d'articles de
    nombreux petits laboratoires. Un rattachement suspect se voit quand un
    seul descendant pèse plus que l'ancêtre lui-même et fournit l'essentiel
    de son volume (part_plus_gros proche de 1).
    """
    data_dir = Path(data_dir)
    con = connect(data_dir)
    _prepare(con, data_dir)
    df = con.execute(f"""
        WITH e AS (                       -- arêtes descendant -> ancêtre (hors soi-même)
            SELECT l.inst_id AS desc_id, l.anc_inst_id AS anc_id
            FROM lin l WHERE l.inst_id <> l.anc_inst_id
        ), c AS (
            SELECT e.anc_id, e.desc_id, coalesce(n.n_direct, 0) AS n_desc
            FROM e LEFT JOIN n_inst n ON n.inst_id = e.desc_id
        ), agg AS (
            SELECT anc_id,
                   sum(n_desc)                    AS n_descendants,
                   count(*)                       AS nb_descendants,
                   arg_max(desc_id, n_desc)       AS plus_gros_id,
                   max(n_desc)                    AS n_plus_gros
            FROM c GROUP BY anc_id
        )
        SELECT a.ror, a.display_name AS ancetre, a.type,
               coalesce(na.n_direct, 0)                    AS n_direct,
               agg.nb_descendants,
               d.display_name                              AS plus_gros_descendant,
               d.ror                                       AS ror_descendant,
               agg.n_plus_gros,
               round(agg.n_plus_gros / nullif(agg.n_descendants, 0), 2) AS part_plus_gros
        FROM agg
        JOIN inst a ON a.inst_id = agg.anc_id
        JOIN inst d ON d.inst_id = agg.plus_gros_id
        LEFT JOIN n_inst na ON na.inst_id = agg.anc_id
        WHERE a.country_code IN ({pays_list_sql(pays)})
          AND agg.n_plus_gros >= {int(min_articles)}
          AND agg.n_plus_gros > coalesce(na.n_direct, 0)
        ORDER BY agg.n_plus_gros DESC
        LIMIT {int(top)}
    """).df()
    con.close()
    return df


def lineage_report(ror: str, data_dir: str = DATA_DIR, top: int = 15) -> dict:
    """Pour un ROR : institution(s), ancêtres, et principaux descendants
    (ceux qui lui apportent des articles via la lignée)."""
    data_dir = Path(data_dir)
    con = connect(data_dir)
    _prepare(con, data_dir)
    ror = ror.split("/")[-1]
    res = {
        "institution": con.execute(f"""
            SELECT i.inst_id, i.display_name, i.type, i.country_code,
                   coalesce(n.n_direct, 0) AS n_direct
            FROM inst i LEFT JOIN n_inst n USING (inst_id) WHERE i.ror = '{ror}'
        """).df(),
        "ancetres": con.execute(f"""
            SELECT a.ror, a.display_name, a.type, a.country_code
            FROM inst i JOIN lin l ON l.inst_id = i.inst_id
            JOIN inst a ON a.inst_id = l.anc_inst_id
            WHERE i.ror = '{ror}' AND l.anc_inst_id <> l.inst_id
        """).df(),
        "descendants": con.execute(f"""
            SELECT d.ror, d.display_name, d.type, coalesce(n.n_direct, 0) AS n_direct,
                   list_transform(d.lineage, x -> x) AS lignee_ids
            FROM inst a JOIN lin l ON l.anc_inst_id = a.inst_id
            JOIN inst d ON d.inst_id = l.inst_id
            LEFT JOIN n_inst n ON n.inst_id = d.inst_id
            WHERE a.ror = '{ror}' AND l.inst_id <> l.anc_inst_id
            ORDER BY n_direct DESC LIMIT {int(top)}
        """).df(),
    }
    con.close()
    print("Institution :"); print(res["institution"].to_string(index=False))
    print("\nAncêtres (tutelles selon OpenAlex) :"); print(res["ancetres"].to_string(index=False))
    print(f"\n{top} principaux descendants (lui apportent leurs articles) :")
    print(res["descendants"].drop(columns="lignee_ids").to_string(index=False))
    return res


def apply_lineage_cuts(con, data_dir, cuts=LINEAGE_CUTS, table: str = "lin_ok") -> None:
    """Crée la table `table` (inst_id, anc_inst_id, anc_ror) : la lignée de
    <data_dir>/inst_lineage.parquet sans les liens coupés par `cuts`."""
    data_dir = Path(data_dir)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _lin_raw AS
        SELECT * FROM read_parquet('{data_dir}/inst_lineage.parquet')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _inst_ror AS
        SELECT inst_id, ror FROM read_parquet('{data_dir}/institutions.parquet')
    """)
    rows = ", ".join(f"('{c.split('/')[-1]}', '{a.split('/')[-1]}')" for c, a in (cuts or []))
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cuts AS
        SELECT * FROM (VALUES {rows or "(NULL, NULL)"}) t(c_ror, a_ror)
        WHERE c_ror IS NOT NULL
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _retires AS
        SELECT DISTINCT x.inst_id, aa.anc_inst_id, k.c_ror, k.a_ror
        FROM _cuts k
        JOIN _inst_ror ci ON ci.ror = k.c_ror
        JOIN _inst_ror ai ON ai.ror = k.a_ror
        JOIN _lin_raw x  ON x.anc_inst_id = ci.inst_id          -- X a C dans sa lignée
        JOIN _lin_raw aa ON aa.inst_id = ai.inst_id              -- A et ses tutelles
        WHERE x.inst_id <> aa.anc_inst_id                        -- X ne se retire pas lui-même
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table} AS
        SELECT l.* FROM _lin_raw l
        ANTI JOIN (SELECT DISTINCT inst_id, anc_inst_id FROM _retires) r
             USING (inst_id, anc_inst_id)
    """)


def preview_lineage_cuts(data_dir: str = DATA_DIR, cuts=LINEAGE_CUTS):
    """Ce que retirent les corrections : pour chaque (C, A), les tutelles
    retirées, le nombre d'institutions et d'articles qui ne leur sont plus
    rattachés. À vérifier avant de relancer compute_indicators()."""
    data_dir = Path(data_dir)
    con = connect(data_dir)
    _prepare(con, data_dir)
    apply_lineage_cuts(con, data_dir, cuts)
    df = con.execute(f"""
        SELECT r.c_ror AS institution, ci.display_name AS nom_institution,
               r.a_ror AS tutelle_coupee, ca.display_name AS nom_tutelle_coupee,
               t.ror AS tutelle_retiree, t.display_name AS nom_tutelle_retiree,
               count(DISTINCT r.inst_id) AS nb_institutions,
               count(DISTINCT wi.work_id) AS nb_articles
        FROM _retires r
        JOIN inst t ON t.inst_id = r.anc_inst_id
        LEFT JOIN inst ci ON ci.ror = r.c_ror
        LEFT JOIN inst ca ON ca.ror = r.a_ror
        LEFT JOIN read_parquet('{data_dir}/work_inst/*.parquet') wi ON wi.inst_id = r.inst_id
        GROUP BY ALL ORDER BY institution, tutelle_coupee, nb_articles DESC
    """).df()
    con.close()
    print(df.to_string(index=False) if len(df) else "Aucun lien retiré (vérifier les ROR de LINEAGE_CUTS)")
    return df
