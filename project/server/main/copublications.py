"""
copublications.py
=================
Étape 6b : avec qui chaque pays et chaque établissement copublie.

Pour chaque entité (pays retenu ou établissement retenu à l'étape 6), et
chaque partenaire, le nombre d'articles cosignés, par périmètre x année x
discipline. Même périmètre, mêmes entités et même comptage entier qu'à
l'étape 6 (un article cosigné compte 1 pour chaque paire).

Paires calculées
----------------
  pays           x pays           pays différents présents sur l'article
  etablissement  x pays           pays d'un coauteur, SAUF le pays de
                                  l'établissement (un article d'un établissement
                                  français a toujours la France parmi ses pays)
  etablissement  x etablissement  autre établissement retenu présent sur l'article,
                                  à condition que l'article ait au moins une affiliation
                                  propre à chacun des deux : une institution qui compte
                                  pour E mais pas pour P, ET une qui compte pour P mais
                                  pas pour E. Un article signé par une seule UMR en
                                  cotutelle CNRS-université n'est donc PAS une
                                  copublication CNRS-université ; une UMR et sa tutelle
                                  ne sont jamais partenaires.
  Pour garder une taille raisonnable, on ne garde par entité et par type de
  partenaire que les COPUB_PARTENAIRES_N partenaires les plus fréquents sur
  toute la période (premier périmètre). Les partenaires établissements sont
  pris parmi les établissements retenus à l'étape 6.

Colonnes de <classement>_copublications.parquet
-----------------------------------------------
  perimetre, classement, entite_type, entite_id,
  partenaire_type (pays | etablissement), partenaire_id,
  annee_publication, discipline_code,
  nb_articles_copublies               articles cosignés par l'entité et le partenaire
  nb_articles_copublies_top10_cites   dont dans le top 10 % des plus cités (éq.-articles)

  Part des copublications avec un partenaire P pour une entité E :
    nb_articles_copublies(E, P) / nb_articles(E)    (nb_articles : étape 6)

Exemple :
    from project.server.main.copublications import compute_copublications
    res = compute_copublications()
    res["france"]          # principaux partenaires de la France, toute la période
"""

import json
from pathlib import Path

from .common import connect
from .config import (ANNEE_MAX, ANNEE_MIN, CLASSEMENT, COPUB_MAX_ENTITES_PAR_ARTICLE,
                     COPUB_PARTENAIRES_N, DATA_DIR, ETAB_COUNTRIES, ETAB_EXCLUDE,
                     ETAB_INTERNATIONAL_N, ETAB_INTERNATIONAL_RORS, ETAB_INTERNATIONAL_TYPES,
                     LINEAGE_CUTS, PAYS_FR, PERIMETRES)
from .indicators import _preparer_articles, _preparer_etablissements


def compute_copublications(classement: str = CLASSEMENT,
                           perimetres=PERIMETRES,
                           revues_tete_name: str = None,
                           data_dir: str = DATA_DIR,
                           pays=PAYS_FR,
                           etab_countries=ETAB_COUNTRIES,
                           lineage_cuts=LINEAGE_CUTS,
                           etab_exclude=ETAB_EXCLUDE,
                           etab_international_rors=ETAB_INTERNATIONAL_RORS,
                           etab_international_n: int = ETAB_INTERNATIONAL_N,
                           etab_international_types=ETAB_INTERNATIONAL_TYPES,
                           partenaires_n: int = COPUB_PARTENAIRES_N,
                           max_entites_par_article: int = COPUB_MAX_ENTITES_PAR_ARTICLE,
                           annee_min: int = ANNEE_MIN,
                           annee_max: int = ANNEE_MAX,
                           out_name: str = None,
                           verbose: bool = True) -> dict:
    """Calcule les copublications (voir l'en-tête du module).
    Les entités sont celles de l'étape 6 (<classement>_entites.parquet) : lancer
    compute_indicators() d'abord, avec les mêmes paramètres d'établissements.

    partenaires_n           : partenaires gardés par entité et par type (None = tous)
    max_entites_par_article : ignore, pour les paires établissement x établissement,
                              les articles qui comptent plus d'établissements retenus
                              (grandes collaborations) ; None = aucun filtre
    Renvoie {'fichier', 'nb_lignes', 'france', 'par_type'}.
    """
    data_dir = Path(data_dir)
    out_name = out_name or classement
    rt_name = revues_tete_name or classement
    out_dir = data_dir / "indicateurs"
    f_ent = out_dir / f"{out_name}_entites.parquet"
    if not f_ent.exists():
        raise FileNotFoundError(f"{f_ent} absent : lancer compute_indicators() d'abord")
    f_rt = data_dir / "revues_tete" / f"{rt_name}.parquet"
    f_ss = data_dir / "revues_tete" / "sources_sjr.parquet"
    con = connect(data_dir)

    # Entités retenues à l'étape 6
    con.execute(f"""
        CREATE TEMP TABLE pays_retenus AS
        SELECT entite_id AS pays_code FROM read_parquet('{f_ent}')
        WHERE entite_type = 'pays' AND pays_compare
    """)
    etab = bool(etab_countries)
    if etab:
        con.execute(f"""
            CREATE TEMP TABLE etablissements_retenus AS
            SELECT entite_id, pays_code FROM read_parquet('{f_ent}') WHERE entite_type = 'etablissement'
        """)
        _preparer_etablissements(con, data_dir, etab_countries, lineage_cuts, etab_exclude,
                                 etab_international_rors, etab_international_n,
                                 etab_international_types)

    filtre_max = (f"""AND work_id IN (SELECT work_id FROM articles_etablissements_retenus
                                     GROUP BY work_id HAVING count(*) <= {int(max_entites_par_article)})"""
                  if max_entites_par_article else "")
    resultats, partenaires_fixes = [], False
    for perimetre in perimetres:
        _preparer_articles(con, data_dir, classement, perimetre, f_rt, f_ss, pays, annee_min, annee_max)
        # Paires (entité, partenaire, article). Pays x pays : auto-jointure de
        # articles_pays sur l'article. Exemple : article A [FR, US, DE], entités
        # retenues FR et US -> (FR, US, A), (FR, DE, A), (US, FR, A), (US, DE, A).
        # DE n'est pas retenu comme entité mais peut être partenaire. La paire est
        # orientée : (FR, US) et (US, FR) sont deux lignes, une par point de vue.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE paires AS
            SELECT 'pays' AS entite_type, e.pays_code AS entite_id,
                   'pays' AS partenaire_type, p.pays_code AS partenaire_id, e.work_id
            FROM articles_pays e
            JOIN pays_retenus USING (pays_code)
            JOIN articles_pays p ON p.work_id = e.work_id AND p.pays_code <> e.pays_code
        """)
        if etab:
            # (article, établissement retenu, institutions de l'article qui comptent pour lui).
            # list(DISTINCT ... ORDER BY) : liste triée des institutions, pour comparer
            # ensuite les listes de deux établissements sur le même article.
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE articles_etablissements_retenus AS
                SELECT wi.work_id, c.entite_id, any_value(er.pays_code) AS pays_code,
                       list(DISTINCT wi.inst_id ORDER BY wi.inst_id) AS institutions
                FROM read_parquet('{data_dir}/work_inst/*.parquet') wi
                JOIN cible c USING (inst_id)
                JOIN etablissements_retenus er USING (entite_id)
                WHERE wi.work_id IN (SELECT work_id FROM articles)
                GROUP BY wi.work_id, c.entite_id
            """)
            # Établissement x pays : pays des coauteurs de l'article, sauf celui de
            # l'établissement. Exemple : article [FR, US] du CNRS -> (CNRS, US) seulement.
            con.execute("""
                INSERT INTO paires
                SELECT 'etablissement', e.entite_id, 'pays', p.pays_code, e.work_id
                FROM articles_etablissements_retenus e
                JOIN articles_pays p ON p.work_id = e.work_id
                                    AND p.pays_code <> coalesce(e.pays_code, '')
            """)
            # Établissement x établissement : auto-jointure sur l'article, puis règle des
            # affiliations propres. list_has_all(L1, L2) = toutes les institutions de L2
            # sont dans L1. On exige que ni l'une ni l'autre liste ne contienne l'autre.
            # Exemples, sur un article :
            #   1. une seule affiliation : UMR 7590 (tutelles Sorbonne U. et CNRS)
            #      CNRS [UMR], Sorbonne [UMR] : listes identiques -> pas de paire
            #   2. UMR 7590 + laboratoire L de l'Inserm (hors Sorbonne)
            #      CNRS [UMR], Inserm [L] : chacun a la sienne -> paire CNRS x Inserm
            #      Sorbonne [UMR], CNRS [UMR] -> toujours pas de paire
            #   3. UMR 7590 + affiliation directe à Sorbonne Université (I2)
            #      Sorbonne [I2, UMR], CNRS [UMR] : Sorbonne contient tout CNRS -> pas de paire
            #      (le CNRS n'a rien en propre)
            # filtre_max (option) : ignore les articles aux très nombreux établissements.
            con.execute(f"""
                INSERT INTO paires
                SELECT 'etablissement', e.entite_id, 'etablissement', p.entite_id, e.work_id
                FROM (SELECT * FROM articles_etablissements_retenus WHERE TRUE {filtre_max}) e
                JOIN articles_etablissements_retenus p
                  ON p.work_id = e.work_id AND p.entite_id <> e.entite_id
                -- chacun a au moins une affiliation que l'autre n'a pas
                WHERE NOT list_has_all(p.institutions, e.institutions)
                  AND NOT list_has_all(e.institutions, p.institutions)
            """)

        # Partenaires principaux, fixés sur le premier périmètre (le plus large) : pour
        # chaque entité et chaque type de partenaire, on compte les articles par
        # partenaire sur toute la période, on numérote par nombre décroissant
        # (row_number, à égalité : ordre alphabétique) et on garde les partenaires_n
        # premiers. Exemple, CNRS x pays : US 9 000, DE 6 500, GB 6 400... -> les 50 premiers.
        if not partenaires_fixes:
            limite = f"QUALIFY rang <= {int(partenaires_n)}" if partenaires_n else ""
            con.execute(f"""
                CREATE TEMP TABLE partenaires_retenus AS
                SELECT entite_type, entite_id, partenaire_type, partenaire_id,
                       row_number() OVER (PARTITION BY entite_type, entite_id, partenaire_type
                                          ORDER BY count(*) DESC, partenaire_id) AS rang
                FROM paires GROUP BY entite_type, entite_id, partenaire_type, partenaire_id
                {limite}
            """)
            partenaires_fixes = True

        # Agrégation par année x discipline, pour les seuls partenaires retenus :
        #   nb_articles_copublies              = nombre d'articles de la paire
        #   nb_articles_copublies_top10_cites  = somme de leurs fractions dans le top 10 %
        t = f"copub_{len(resultats)}"
        con.execute(f"""
            CREATE TEMP TABLE {t} AS
            SELECT '{perimetre}' AS perimetre, '{classement}' AS classement,
                   p.entite_type, p.entite_id, p.partenaire_type, p.partenaire_id,
                   a.annee_publication, a.discipline_code,
                   count(*)::BIGINT                     AS nb_articles_copublies,
                   sum(a.fraction_top10_cites)::DOUBLE  AS nb_articles_copublies_top10_cites
            FROM paires p
            JOIN partenaires_retenus USING (entite_type, entite_id, partenaire_type, partenaire_id)
            JOIN articles a USING (work_id)
            GROUP BY ALL
        """)
        resultats.append(t)
        if verbose:
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"[{perimetre}] {n:,} lignes")

    f_out = out_dir / f"{out_name}_copublications.parquet"
    con.execute(f"""
        COPY (SELECT * FROM ({" UNION ALL ".join(f"SELECT * FROM {t}" for t in resultats)})
              ORDER BY perimetre, entite_type, entite_id, partenaire_type, partenaire_id,
                       annee_publication, discipline_code)
        TO '{f_out}' (FORMAT parquet, COMPRESSION zstd)
    """)
    params = dict(classement=classement, perimetres=list(perimetres), partenaires_n=partenaires_n,
                  max_entites_par_article=max_entites_par_article,
                  annee_min=annee_min, annee_max=annee_max)
    (out_dir / f"{out_name}_copublications_params.json").write_text(json.dumps(params, indent=1))

    q = f"read_parquet('{f_out}')"
    res = {
        "fichier": str(f_out),
        "nb_lignes": con.execute(f"SELECT count(*) FROM {q}").fetchone()[0],
        "par_type": con.execute(f"""
            SELECT entite_type, partenaire_type, count(DISTINCT entite_id) AS nb_entites,
                   count(DISTINCT partenaire_id) AS nb_partenaires, count(*) AS nb_lignes
            FROM {q} WHERE perimetre = '{perimetres[0]}' GROUP BY ALL ORDER BY ALL
        """).df(),
        "france": con.execute(f"""
            SELECT c.partenaire_id, sum(c.nb_articles_copublies)::BIGINT AS nb_articles_copublies,
                   round(100 * sum(c.nb_articles_copublies_top10_cites)
                         / sum(c.nb_articles_copublies), 1) AS pct_top10_cites
            FROM {q} c
            WHERE c.entite_id = 'FR' AND c.partenaire_type = 'pays' AND c.perimetre = '{perimetres[0]}'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """).df(),
    }
    con.close()
    if verbose:
        print(f"\n{res['nb_lignes']:,} lignes -> {f_out}\n")
        print(res["par_type"].to_string(index=False))
        print(f"\nPrincipaux pays partenaires de la France ({perimetres[0]}, toute la période) :")
        print(res["france"].to_string(index=False))
    return res
