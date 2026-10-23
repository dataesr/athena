"""
indicators.py
=============
Étape 6 : les effectifs par périmètre x entité x année x discipline.

On ne stocke que des NOMBRES D'ARTICLES (jamais de ratios) : un article n'a
qu'une discipline et qu'une année, on peut donc additionner les effectifs sur
n'importe quelle période ou groupe de disciplines, PUIS diviser. Les ratios
(influence, prestige, poids) sont calculés à la lecture (serveur tableaux,
aperçu HTML, contrôles ci-dessous).

Entités
-------
  monde          entite_id = "MONDE" : tous les articles du périmètre ; c'est le
                 dénominateur de toutes les parts.
  pays           les PAYS_COMPARAISON_N pays qui publient le plus (ou la liste
                 PAYS_COMPARAISON), France toujours comprise. Un article compte
                 pour un pays s'il a au moins un auteur de ce pays (outre-mer
                 regroupé sous FR, voir PAYS_FR).
  etablissement  chaque ROR d'une institution de ETAB_COUNTRIES, plus les
                 établissements étrangers de comparaison. Un article compte pour
                 un ROR si l'une de ses institutions EST ce ROR ou l'a pour
                 tutelle (lignée OpenAlex corrigée par LINEAGE_CUTS) : un article
                 d'une UMR compte pour l'UMR, son université et le CNRS.
  Comptage entier : un article compte 1 pour chaque entité présente.

Colonnes de <classement>_effectifs.parquet
------------------------------------------
  perimetre                     toutes_revues_openalex | revues_sjr
  classement                    ex. erc
  entite_type                   monde | pays | etablissement
  entite_id                     MONDE | code ISO du pays | ROR court
  annee_publication
  discipline_code
  nb_articles                   articles de l'entité
  nb_articles_top10_cites       articles dans le top 10 % des plus cités de la
                                discipline et de l'année, en équivalent-articles
                                (somme de fraction_top10_cites, étape 4)
  nb_articles_revues_tete_disciplinaires     articles parus dans une des revues de
                                tête PROPRES à leur discipline (étape 5)
  nb_articles_revues_tete_multidisciplinaires articles parus dans une revue
                                multidisciplinaire de tête (Nature, Science...)
  influence_provisoire          fenêtre de citation pas encore close

  Ratios (pour une entité E, en sommant d'abord sur les lignes voulues) :
    part_articles     (poids)     = nb_articles(E) / nb_articles(MONDE)
    part_top10_cites  (influence) = nb_articles_top10_cites(E) / nb_articles_top10_cites(MONDE)
    part_revues_tete  (prestige)  = nb_articles_revues_tete(E) / nb_articles_revues_tete(MONDE)
        avec nb_articles_revues_tete = ..._disciplinaires (+ ..._multidisciplinaires
        si l'on inclut les revues multidisciplinaires)

Autres sorties (dans <data_dir>/indicateurs/)
---------------------------------------------
  <classement>_entites.parquet   une ligne par entité : entite_id, entite_type, nom,
                                 pays_code, type_openalex, nb_articles (premier
                                 périmètre, toute la période), pays_compare,
                                 tutelles (ROR), nb_structures_rattachees
  <classement>_params.json       paramètres utilisés

Les fonctions _preparer_* sont réutilisées par copublications.py (étape 6b).

Exemple :
    from project.server.main.indicators import compute_indicators
    res = compute_indicators()
    res["france"]          # France, toutes disciplines, par année et par périmètre
"""

import json
from pathlib import Path

import pandas as pd

from .common import MONDE_ID, PAYS_NOMS, connect, nom_fichier, pays_list_sql
from .config import (ANNEE_MAX, ANNEE_MIN, CLASSEMENT, DATA_DIR, ETAB_COUNTRIES,
                     ETAB_EXCLUDE, ETAB_INTERNATIONAL_N, ETAB_INTERNATIONAL_RORS,
                     ETAB_INTERNATIONAL_TYPES, ETAB_MIN_ARTICLES, LINEAGE_CUTS, PAYS_COMPARAISON,
                     PAYS_COMPARAISON_N, PAYS_FR, PERIMETRES)
from .lineage_check import apply_lineage_cuts

COLONNES_EFFECTIFS = ["nb_articles", "nb_articles_top10_cites",
                      "nb_articles_revues_tete_disciplinaires",
                      "nb_articles_revues_tete_multidisciplinaires"]

# Agrégation commune : une ligne de `articles` = un article, donc
#   count(*)                              = nombre d'articles
#   sum(fraction_top10_cites)             = articles dans le top 10 %, en équivalent-articles
#                                           (un article à moitié dans le top compte 0,5)
#   sum(en_revue_tete_...::INT)           = articles parus dans une revue de tête (vrai = 1)
_AGG = """count(*)::BIGINT                                                AS nb_articles,
          sum(fraction_top10_cites)::DOUBLE                               AS nb_articles_top10_cites,
          sum(en_revue_tete_disciplinaire::INT)::BIGINT                   AS nb_articles_revues_tete_disciplinaires,
          sum(en_revue_tete_multidisciplinaire::INT)::BIGINT              AS nb_articles_revues_tete_multidisciplinaires"""


def _fichier_top10(data_dir: Path, classement: str, perimetre: str) -> tuple:
    name = nom_fichier(classement, perimetre)
    f = data_dir / "top10" / f"{name}.parquet"
    if not f.exists():
        raise FileNotFoundError(f"{f} absent : lancer compute_top10(classement={classement!r}, "
                                f"perimetre={perimetre!r})")
    p = data_dir / "top10" / f"{name}_params.json"
    params = json.loads(p.read_text()) if p.exists() else {}
    return f, params


def _provisoire_sql(params: dict, col: str = "annee_publication") -> str:
    W, years = params.get("citation_window"), params.get("counts_years") or [None, None]
    if W is None or years[1] is None:
        return "FALSE"
    return f"({col} + {int(W)} >= {int(years[1])})"


def _preparer_etablissements(con, data_dir: Path, etab_countries, lineage_cuts, etab_exclude,
                             etab_international_rors, etab_international_n,
                             etab_international_types) -> None:
    """Tables lin_ok (lignée corrigée), inst_all, etranger (ROR étrangers de
    comparaison) et cible (institution -> ROR de l'établissement, elle-même ou
    une tutelle)."""
    apply_lineage_cuts(con, data_dir, lineage_cuts, table="lin_ok")
    excl = pays_list_sql([r.split("/")[-1] for r in (etab_exclude or [])]) or "NULL"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inst_all AS
        SELECT inst_id, ror, display_name, country_code, type
        FROM read_parquet('{data_dir}/institutions.parquet')
    """)
    # Établissements étrangers : liste imposée, ou les N plus gros. Taille approchée
    # (sert seulement à les choisir) : pour chaque ROR étranger, somme des articles
    # de toutes les institutions qui l'ont dans leur lignée (lui-même compris).
    #   nd : articles par institution (count DISTINCT : un article peut citer deux
    #        fois la même institution) ;
    #   lin_ok : (institution, ancêtre) -> on additionne les articles des institutions
    #        sur leur ancêtre a.ror. Un article d'un laboratoire ET de son université
    #        est compté deux fois pour l'université : taille surestimée, sans effet
    #        sur le choix des plus gros.
    if etab_international_rors:
        rors = pays_list_sql([r.split("/")[-1] for r in etab_international_rors])
        con.execute(f"CREATE OR REPLACE TEMP TABLE etranger AS SELECT ror FROM inst_all WHERE ror IN ({rors})")
    elif etab_international_n:
        types = (f"AND a.type IN ({pays_list_sql(etab_international_types)})"
                 if etab_international_types else "")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE etranger AS
            WITH nd AS (
                SELECT inst_id, count(DISTINCT work_id) AS nb_articles
                FROM read_parquet('{data_dir}/work_inst/*.parquet') GROUP BY 1
            )
            SELECT a.ror, sum(nd.nb_articles) AS taille
            FROM lin_ok l
            JOIN nd ON nd.inst_id = l.inst_id
            JOIN inst_all a ON a.inst_id = l.anc_inst_id
            WHERE a.ror IS NOT NULL AND a.ror <> ''
              AND a.country_code NOT IN ({pays_list_sql(etab_countries)}) {types}
            GROUP BY a.ror ORDER BY taille DESC LIMIT {int(etab_international_n)}
        """)
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE etranger AS SELECT NULL::VARCHAR AS ror WHERE FALSE")
    # cible : pour chaque institution OpenAlex, les ROR d'établissements pour
    # lesquels ses articles comptent (elle-même et ses tutelles), restreints aux
    # établissements de ETAB_COUNTRIES et aux étrangers de comparaison, moins les
    # ROR exclus (ETAB_EXCLUDE).
    # Exemple : UMR 7590 (I3), lignée [I3, Sorbonne Université I2, CNRS I1]
    #   -> (I3, ror UMR), (I3, ror Sorbonne), (I3, ror CNRS) : un article de l'UMR
    #   compte pour les trois.
    # Le pays testé est celui de l'ANCÊTRE (i.country_code) : une UMR française
    # rattachée à une tutelle étrangère ne fait pas entrer cette tutelle.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cible AS
        SELECT DISTINCT l.inst_id, l.anc_ror AS entite_id
        FROM lin_ok l
        JOIN inst_all i ON i.inst_id = l.anc_inst_id
        WHERE l.anc_ror IS NOT NULL AND l.anc_ror <> ''
          AND (i.country_code IN ({pays_list_sql(etab_countries)})
               OR l.anc_ror IN (SELECT ror FROM etranger))
          AND l.anc_ror NOT IN ({excl})
    """)


def _preparer_articles(con, data_dir: Path, classement: str, perimetre: str,
                       f_revues_tete: Path, f_sources_sjr: Path, pays, annee_min: int,
                       annee_max: int) -> dict:
    """Table `articles` du périmètre : une ligne par article
      work_id, annee_publication, discipline_code, fraction_top10_cites,
      pays_codes (outre-mer regroupé sous FR), en_revue_tete_disciplinaire,
      en_revue_tete_multidisciplinaire
    et table `articles_pays` (work_id, pays_code). Renvoie les paramètres du top 10 %."""
    f_top10, params10 = _fichier_top10(data_dir, classement, perimetre)
    fr = pays_list_sql(pays)
    # revues_tete : (discipline, revue SJR) -> disciplinaire vrai / faux.
    # max(...) : par sécurité si une revue figurait deux fois pour une discipline
    # (l'étape 5 l'évite déjà) ; elle est alors considérée comme disciplinaire.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE revues_tete AS
        SELECT discipline_code, sjr_id,
               max((categorie = 'disciplinaire')::INT) = 1 AS disciplinaire
        FROM read_parquet('{f_revues_tete}') GROUP BY ALL
    """)
    # articles : une ligne par article du périmètre (fichier top10 de l'étape 4,
    # qui contient déjà la discipline et la fraction dans le top).
    #   pays_codes : pays des auteurs, les codes d'outre-mer (GP, RE...) remplacés
    #     par FR, puis dédoublonnés. Exemple : [FR, GP, US] -> [FR, FR, US] -> [FR, US].
    #   en_revue_tete_disciplinaire / _multidisciplinaire : la revue de l'article
    #     (source OpenAlex -> revue SJR) est-elle une revue de tête DE SA discipline ?
    #     Jointure sur (discipline, revue) : un article de Nature classé en PE10 ne
    #     regarde que les revues de tête de PE10. Pas de revue de tête -> NULL ->
    #     coalesce -> faux pour les deux colonnes.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE articles AS
        SELECT t.work_id, t.annee_publication, t.discipline_code, t.fraction_top10_cites,
               list_distinct(list_transform(coalesce(w.countries, []),
                   c -> CASE WHEN c IN ({fr}) THEN 'FR' ELSE c END))       AS pays_codes,
               coalesce(rt.disciplinaire, false)                           AS en_revue_tete_disciplinaire,
               coalesce(NOT rt.disciplinaire, false)                       AS en_revue_tete_multidisciplinaire
        FROM read_parquet('{f_top10}') t
        JOIN (SELECT work_id, countries, source_id
              FROM read_parquet('{data_dir}/works/*.parquet')) w USING (work_id)
        LEFT JOIN read_parquet('{f_sources_sjr}') ss ON ss.source_id = w.source_id
        LEFT JOIN revues_tete rt ON rt.discipline_code = t.discipline_code AND rt.sjr_id = ss.sjr_id
        WHERE t.annee_publication BETWEEN {int(annee_min)} AND {int(annee_max)}
    """)
    # articles_pays : une ligne par (article, pays). Comptage entier : un article
    # FR-US compte 1 pour la France ET 1 pour les États-Unis.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE articles_pays AS
        SELECT work_id, unnest(pays_codes) AS pays_code FROM articles
    """)
    return params10


def _preparer_articles_etablissements(con, data_dir: Path) -> None:
    """Table `articles_etablissements` (work_id, entite_id) des articles du périmètre."""
    # work_inst (article, institution) x cible (institution, établissement) :
    # chaque article est relié à tous les établissements de toutes ses institutions.
    # DISTINCT : un article avec deux laboratoires de la même université ne compte
    # qu'une fois pour l'université.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE articles_etablissements AS
        SELECT DISTINCT wi.work_id, c.entite_id
        FROM read_parquet('{data_dir}/work_inst/*.parquet') wi
        JOIN cible c USING (inst_id)
        WHERE wi.work_id IN (SELECT work_id FROM articles)
    """)


def _retenir_entites(con, pays_comparaison, pays_comparaison_n: int, etab: bool,
                     etab_min_articles: int) -> None:
    """Tables pays_retenus et etablissements_retenus, fixées sur le premier
    périmètre (le plus large). La France est toujours retenue."""
    if pays_comparaison:
        liste = sorted({"FR", *[p.upper() for p in pays_comparaison]})
        con.execute("CREATE TEMP TABLE pays_retenus AS SELECT unnest(?::VARCHAR[]) AS pays_code", [liste])
    else:
        # Les N pays qui ont le plus d'articles sur toute la période (premier périmètre)
        con.execute(f"""
            CREATE TEMP TABLE pays_retenus AS
            SELECT pays_code FROM articles_pays
            GROUP BY pays_code ORDER BY count(*) DESC LIMIT {int(pays_comparaison_n)}
        """)
        con.execute("INSERT INTO pays_retenus SELECT 'FR' WHERE 'FR' NOT IN (SELECT pays_code FROM pays_retenus)")
    if etab:
        # Établissements avec au moins etab_min_articles articles sur la période
        con.execute(f"""
            CREATE TEMP TABLE etablissements_retenus AS
            SELECT entite_id, count(*) AS nb_articles FROM articles_etablissements
            GROUP BY entite_id HAVING count(*) >= {int(etab_min_articles)}
        """)


def compute_indicators(classement: str = CLASSEMENT,
                       perimetres=PERIMETRES,
                       revues_tete_name: str = None,
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
    """Calcule les effectifs (voir l'en-tête du module).

    classement        : classement des disciplines (étapes 3 à 5)
    perimetres        : périmètres à calculer ; chacun exige son fichier top10
    revues_tete_name  : liste de revues de tête (défaut : nom du classement)
    etab_countries    : None = pas d'établissements (pays seulement)
    lineage_cuts      : corrections de la lignée OpenAlex (voir config.LINEAGE_CUTS)
    etab_exclude      : ROR à ne pas présenter comme établissements
    pays_comparaison  : pays retenus (None = les pays_comparaison_n plus gros)
    etab_international_rors / _n / _types : établissements étrangers de comparaison
    out_name          : défaut : nom du classement

    Renvoie {'fichier', 'nb_lignes', 'nb_etablissements', 'france', 'top_etablissements', 'pays'}.
    """
    data_dir = Path(data_dir)
    out_name = out_name or classement
    rt_name = revues_tete_name or classement
    out_dir = data_dir / "indicateurs"
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(data_dir)

    f_rt = data_dir / "revues_tete" / f"{rt_name}.parquet"
    f_ss = data_dir / "revues_tete" / "sources_sjr.parquet"
    for f in (f_rt, f_ss):
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : lancer compute_top_journals()")

    etab = bool(etab_countries)
    if etab:
        _preparer_etablissements(con, data_dir, etab_countries, lineage_cuts, etab_exclude,
                                 etab_international_rors, etab_international_n,
                                 etab_international_types)

    resultats, premier = [], True
    for perimetre in perimetres:
        params10 = _preparer_articles(con, data_dir, classement, perimetre, f_rt, f_ss, pays,
                                      annee_min, annee_max)
        if verbose:
            n = con.execute("SELECT count(*) FROM articles").fetchone()[0]
            print(f"[{perimetre}] {n:,} articles")
        if etab:
            _preparer_articles_etablissements(con, data_dir)
        if premier:
            _retenir_entites(con, pays_comparaison, pays_comparaison_n, etab, etab_min_articles)
            # nombre d'articles de tous les pays (noms et tailles dans la table des entités)
            con.execute("""
                CREATE TEMP TABLE taille_pays AS
                SELECT pays_code, count(*) AS nb_articles FROM articles_pays GROUP BY 1
            """)

        # Effectifs du périmètre : une table par périmètre, trois blocs.
        #   1. MONDE : tous les articles, par année x discipline (dénominateurs) ;
        #   2. pays retenus : articles_pays -> articles (comptage entier) ;
        #   3. établissements retenus : articles_etablissements -> articles.
        # Exemple (une année, une discipline) : 3 articles
        #   A1 [FR, US], top 1,   revue de tête disciplinaire
        #   A2 [FR],     top 0,   pas en revue de tête
        #   A3 [US],     top 0,5, revue multidisciplinaire
        #   MONDE : nb_articles 3, top10 1,5, rt_disc 1, rt_multi 1
        #   FR    : nb_articles 2, top10 1,   rt_disc 1, rt_multi 0
        #   US    : nb_articles 2, top10 1,5, rt_disc 1, rt_multi 1
        #   -> influence FR = 1 / 1,5 = 67 % ; les parts FR et US se recouvrent (A1).
        t = f"effectifs_{len(resultats)}"
        prov = _provisoire_sql(params10)
        con.execute(f"""
            CREATE TEMP TABLE {t} AS
            SELECT 'monde' AS entite_type, '{MONDE_ID}' AS entite_id,
                   annee_publication, discipline_code, {_AGG}
            FROM articles GROUP BY ALL
        """)
        con.execute(f"""
            INSERT INTO {t}
            SELECT 'pays', ap.pays_code, a.annee_publication, a.discipline_code, {_AGG}
            FROM articles_pays ap
            JOIN pays_retenus USING (pays_code)
            JOIN articles a USING (work_id)
            GROUP BY ALL
        """)
        if etab:
            con.execute(f"""
                INSERT INTO {t}
                SELECT 'etablissement', ae.entite_id, a.annee_publication, a.discipline_code, {_AGG}
                FROM articles_etablissements ae
                JOIN etablissements_retenus USING (entite_id)
                JOIN articles a USING (work_id)
                GROUP BY ALL
            """)
        # On ajoute les colonnes communes : périmètre, classement, influence provisoire
        # (année + fenêtre de citation >= dernière année de citations disponible).
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {t} AS
            SELECT '{perimetre}' AS perimetre, '{classement}' AS classement, *,
                   {prov} AS influence_provisoire
            FROM {t}
        """)
        resultats.append(t)
        premier = False

    # --- Écriture des effectifs ----------------------------------------------------
    union = " UNION ALL ".join(f"SELECT * FROM {t}" for t in resultats)
    f_out = out_dir / f"{out_name}_effectifs.parquet"
    con.execute(f"""
        COPY (
            SELECT perimetre, classement, entite_type, entite_id, annee_publication,
                   discipline_code, {", ".join(COLONNES_EFFECTIFS)}, influence_provisoire
            FROM ({union})
            ORDER BY perimetre, entite_type, entite_id, annee_publication, discipline_code
        ) TO '{f_out}' (FORMAT parquet, COMPRESSION zstd)
    """)

    # --- Table des entités -----------------------------------------------------------
    # Une requête par type d'entité, réunies par UNION ALL (mêmes colonnes) :
    #   monde : une ligne ;
    #   pays  : TOUS les pays vus dans les articles (y compris non retenus : ils
    #           servent de partenaires dans les copublications), pays_compare = retenu ;
    #   établissements retenus, avec :
    #     nom, pays, type   : institutions OpenAlex (any_value : le ROR est unique) ;
    #     tutelles          : ROR des ancêtres de l'institution, sauf elle-même ;
    #     nb_structures_rattachees : institutions qui l'ont dans leur lignée, sauf
    #                         elle-même (- 1 : la ligne (X, X)).
    con.register("noms_pays", pd.DataFrame(list(PAYS_NOMS.items()), columns=["pays_code", "nom"]))
    total_monde = con.execute(f"SELECT sum(nb_articles) FROM {resultats[0]} "
                              f"WHERE entite_type = 'monde'").fetchone()[0]
    requetes = [f"""
        SELECT '{MONDE_ID}' AS entite_id, 'monde' AS entite_type, 'Monde' AS nom,
               NULL::VARCHAR AS pays_code, NULL::VARCHAR AS type_openalex,
               {int(total_monde)}::BIGINT AS nb_articles, false AS pays_compare,
               []::VARCHAR[] AS tutelles, 0::BIGINT AS nb_structures_rattachees
    """, """
        SELECT tp.pays_code, 'pays', coalesce(np.nom, tp.pays_code), tp.pays_code, NULL,
               tp.nb_articles, tp.pays_code IN (SELECT pays_code FROM pays_retenus),
               []::VARCHAR[], 0
        FROM taille_pays tp LEFT JOIN noms_pays np USING (pays_code)
    """]
    if etab:
        requetes.append("""
            SELECT er.entite_id, 'etablissement', coalesce(i.nom, er.entite_id), i.pays_code,
                   i.type_openalex, er.nb_articles, false,
                   coalesce(tu.tutelles, []), coalesce(ra.nb, 0)
            FROM etablissements_retenus er
            LEFT JOIN (SELECT ror AS entite_id, any_value(display_name) AS nom,
                              any_value(country_code) AS pays_code, any_value(type) AS type_openalex
                       FROM inst_all WHERE ror IS NOT NULL AND ror <> '' GROUP BY ror) i USING (entite_id)
            LEFT JOIN (SELECT i.ror AS entite_id, list(DISTINCT l.anc_ror ORDER BY l.anc_ror) AS tutelles
                       FROM lin_ok l JOIN inst_all i ON i.inst_id = l.inst_id
                       WHERE l.anc_ror IS NOT NULL AND l.anc_ror <> '' AND l.anc_ror <> i.ror
                       GROUP BY i.ror) tu USING (entite_id)
            LEFT JOIN (SELECT l.anc_ror AS entite_id, count(DISTINCT l.inst_id) - 1 AS nb
                       FROM lin_ok l GROUP BY l.anc_ror) ra USING (entite_id)
        """)
    f_ent = out_dir / f"{out_name}_entites.parquet"
    con.execute(f"""
        COPY (SELECT * FROM ({" UNION ALL ".join(requetes)})
              ORDER BY CASE entite_type WHEN 'monde' THEN 0 WHEN 'pays' THEN 1 ELSE 2 END,
                       nb_articles DESC)
        TO '{f_ent}' (FORMAT parquet)
    """)
    n_etab = con.execute("SELECT count(*) FROM etablissements_retenus").fetchone()[0] if etab else 0

    params = dict(classement=classement, perimetres=list(perimetres), revues_tete=rt_name,
                  pays_fr=list(pays), etab_countries=list(etab_countries or []),
                  etab_min_articles=etab_min_articles,
                  lineage_cuts=[list(c) for c in (lineage_cuts or [])],
                  etab_exclude=list(etab_exclude or []), annee_min=annee_min, annee_max=annee_max,
                  pays_compares=[r[0] for r in con.execute(
                      "SELECT pays_code FROM pays_retenus ORDER BY 1").fetchall()])
    (out_dir / f"{out_name}_params.json").write_text(json.dumps(params, indent=1))

    # --- Contrôles : ratios recalculés à partir des effectifs -------------------------
    # avec_monde : chaque ligne d'effectifs à côté de la ligne MONDE de même
    # périmètre, année et discipline (préfixe m_ = monde). Les ratios additionnent
    # d'abord sur les lignes voulues (sum), puis divisent : jamais de moyenne de parts.
    q = f"read_parquet('{f_out}')"
    con.execute(f"""
        CREATE TEMP VIEW avec_monde AS
        SELECT e.*, m.nb_articles AS m_articles, m.nb_articles_top10_cites AS m_top10,
               m.nb_articles_revues_tete_disciplinaires AS m_rt_disc,
               m.nb_articles_revues_tete_multidisciplinaires AS m_rt_multi
        FROM {q} e JOIN {q} m
          ON m.entite_id = '{MONDE_ID}' AND m.perimetre = e.perimetre
         AND m.annee_publication = e.annee_publication AND m.discipline_code = e.discipline_code
    """)
    ratios = """round(100 * sum(nb_articles) / sum(m_articles), 2)                     AS part_articles_pct,
                round(100 * sum(nb_articles_top10_cites) / sum(m_top10), 2)           AS part_top10_cites_pct,
                round(100 * sum(nb_articles_revues_tete_disciplinaires + nb_articles_revues_tete_multidisciplinaires)
                      / sum(m_rt_disc + m_rt_multi), 2)                               AS part_revues_tete_pct,
                round(100 * sum(nb_articles_revues_tete_disciplinaires) / sum(m_rt_disc), 2)
                                                                                      AS part_revues_tete_disciplinaires_pct"""
    res = {
        "fichier": str(f_out),
        "fichier_entites": str(f_ent),
        "nb_lignes": con.execute(f"SELECT count(*) FROM {q}").fetchone()[0],
        "nb_etablissements": n_etab,
        "france": con.execute(f"""
            SELECT perimetre, annee_publication, sum(nb_articles)::BIGINT AS nb_articles,
                   {ratios},
                   round(100 * sum(m_top10) / sum(m_articles), 2) AS controle_top10_monde_pct,
                   bool_or(influence_provisoire) AS influence_provisoire
            FROM avec_monde WHERE entite_id = 'FR'
            GROUP BY ALL ORDER BY perimetre, annee_publication
        """).df(),
        "top_etablissements": con.execute(f"""
            SELECT entite_id AS ror, any_value(nom) AS nom, sum(nb_articles)::BIGINT AS nb_articles,
                   {ratios}
            FROM avec_monde
            JOIN (SELECT entite_id, nom, pays_code FROM read_parquet('{f_ent}')) USING (entite_id)
            WHERE entite_type = 'etablissement' AND perimetre = '{perimetres[0]}' AND pays_code = 'FR'
            GROUP BY 1 ORDER BY nb_articles DESC LIMIT 15
        """).df() if n_etab else None,
        "pays": con.execute(f"""
            SELECT entite_id AS pays_code,
                   round(100 * sum(nb_articles_top10_cites) FILTER (WHERE annee_publication = {int(annee_min)})
                         / sum(m_top10) FILTER (WHERE annee_publication = {int(annee_min)}), 2)
                       AS part_top10_cites_debut_pct,
                   round(100 * sum(nb_articles_top10_cites) FILTER (WHERE annee_publication = {int(annee_max)})
                         / sum(m_top10) FILTER (WHERE annee_publication = {int(annee_max)}), 2)
                       AS part_top10_cites_fin_pct
            FROM avec_monde WHERE entite_type = 'pays' AND perimetre = '{perimetres[-1]}'
            GROUP BY 1 ORDER BY part_top10_cites_fin_pct DESC
        """).df(),
    }
    con.close()

    if verbose:
        print(f"\n{res['nb_lignes']:,} lignes -> {f_out} ({n_etab:,} établissements, "
              f"{len(res['pays'])} pays)")
        print(f"Entités -> {f_ent}")
        print("\nFrance, toutes disciplines (en %) :")
        print(res["france"].to_string(index=False))
        if res["top_etablissements"] is not None:
            print(f"\n15 plus gros établissements français, périmètre {perimetres[0]}, "
                  f"toute la période (en %) :")
            print(res["top_etablissements"].to_string(index=False))
        print(f"\nPays retenus, part du top 10 % des plus cités (périmètre {perimetres[-1]}, en %) :")
        print(res["pays"].to_string(index=False))
    return res
