"""
viviers.py
==========
Viviers de chercheurs : chercheurs « actifs » par pays, par établissement français et par
discipline (panel ERC), d'après leurs publications OpenAlex. Dénominateur commun aux ERC, aux
projets (Horizon Europe, ANR...) et aux prix : candidatures ou lauréats rapportés au vivier, et
comparaison du taux de candidature ERC de la France à celui des autres pays.

Un « chercheur » est un auteur OpenAlex (author_id, désambiguïsation d'OpenAlex). Pays lus :
la France (outre-mer compris, PAYS_FR), les pays de la comparaison ERC (erc_meta.pays_compares)
et ceux de la comparaison bibliométrique (biblio_meta.pays_compares), ou la liste VIVIER_PAYS :
lancer erc.compute_erc() et indicators.compute_indicators() avant l'extraction.

Deux étapes, après l'étape 4 (top10/) et de préférence l'étape 6 (indicateurs/<classement>_entites) :

1. extraire_auteurs()   (long : relit les works du snapshot sur S3, de l'ordre de l'étape 1 ;
   reprise possible, fichier par fichier)
   Une ligne par (article, auteur) dont l'auteur a sur l'article une institution d'un des pays
   lus. Mêmes filtres que l'étape 1 (types article et review, ni rétracté ni paratexte, avec un
   topic), depuis VIVIER_PUB_ANNEE_MIN. Écrit viviers/auteurs/<fichier>.parquet :
     work_id, year, nb_auteurs, author_id, orcid, nom, position_auteur, correspondant,
     pays (pays lus de ses institutions, outre-mer sous FR),
     inst_ids (toutes ses institutions sur l'article ; seulement si FR est dans pays, sinon null)
   Si la liste des pays change (nouveau calcul ERC), tout est relu.
   Lecture partagée avec les prix : publications_auteurs.extraire_publications() écrit les
   deux sorties en une seule lecture des works (voir pipeline, étape "publications").

2. compute_viviers()    (quelques minutes, local)
   Articles retenus : ceux du fichier top10 du périmètre VIVIER_PERIMETRE (articles de revues,
   avec discipline et fraction_top10_cites), publiés dans la fenêtre
   [VIVIER_ANNEE_DEBUT, VIVIER_ANNEE_FIN], à au plus
   VIVIER_MAX_AUTEURS_PAR_ARTICLE auteurs.
   Chercheur actif : au moins VIVIER_MIN_ARTICLES articles dans la fenêtre, dont au moins
   VIVIER_MIN_ARTICLES_RECENTS dans les VIVIER_ANNEES_RECENTES dernières années.
   Activité et parts se calculent sur toutes ses signatures lues (tous pays lus confondus).
   Pays : ceux où sont affiliés au moins VIVIER_PART_MIN_PAYS de ses articles récents ; même
   règle pour tous les pays, France comprise.
   Établissements français (ROR, lignée OpenAlex corrigée comme à l'étape 6) : ceux auxquels
   sont affiliés au moins VIVIER_PART_MIN_ETABLISSEMENT de ses articles récents ; un chercheur
   d'UMR compte pour l'UMR, ses universités et le CNRS.
   Discipline : publications_auteurs.poids_disciplines(), même règle que les lauréats de prix :
   part de ses articles de la période par panel ; discipline principale = la plus fréquente
   (à égalité : code le plus petit).
   Top 10 % : articles dont fraction_top10_cites >= VIVIER_SEUIL_FRACTION_TOP10 ; chercheur
   « top 10 % » s'il en a au moins VIVIER_MIN_TOP10 dans la fenêtre.
   Auteur principal : premier ou dernier auteur, ou auteur correspondant.
   Écrit dans viviers/ :
     viviers_chercheurs.parquet  un document par chercheur actif rattaché à la France ou à un
                                 établissement français (voir CONTRAT_DONNEES.md)
     viviers_effectifs.parquet   entite_type x entite_id x discipline_code : nb_chercheurs
                                 (discipline principale), nb_chercheurs_fractionnaire,
                                 nb_chercheurs_top10, nb_chercheurs_top10_fractionnaire,
                                 nb_chercheurs_auteur_principal
     viviers_meta.json           période, paramètres, contrôles, textes de méthode

Limites : seules les signatures dans les pays lus sont vues (un article d'un chercheur publié
depuis un autre pays ne compte pas) ; la qualité dépend de la désambiguïsation des auteurs
OpenAlex (un chercheur peut être scindé en plusieurs author_id).

Exemple :
    from project.server.main.viviers import extraire_auteurs, compute_viviers
    extraire_auteurs()             # long, reprise possible
    compute_viviers()
    # autre fenêtre : extraire depuis son début (relit les works si annee_min change)
    extraire_auteurs(annee_min=2016); compute_viviers(annee_debut=2016, annee_fin=2020, out_suffix="_2016_2020")
"""

import datetime as dt
import json
from pathlib import Path

from .common import PAYS_NOMS, connect
from .config import (CLASSEMENT, DATA_DIR, ERC_DIR, ETAB_COUNTRIES, ETAB_EXCLUDE, LINEAGE_CUTS, PAYS_FR, SNAPSHOT,
                     VIVIER_ANNEE_FIN, VIVIER_PART_MIN_PAYS, VIVIER_PAYS, VIVIER_ANNEES_RECENTES, VIVIER_ANNEE_DEBUT,
                     VIVIER_MAX_AUTEURS_PAR_ARTICLE, VIVIER_MIN_ARTICLES, VIVIER_MIN_ARTICLES_RECENTS,
                     VIVIER_MIN_AUTEUR_PRINCIPAL, VIVIER_MIN_TOP10, VIVIER_PART_MIN_ETABLISSEMENT,
                     VIVIER_PERIMETRE, VIVIER_PUB_ANNEE_MIN, VIVIER_SEUIL_FRACTION_TOP10, VIVIERS_DIR)
from .publications_auteurs import TYPES, extraire_publications, poids_disciplines


def pays_viviers(erc_dir: str = ERC_DIR, pays=VIVIER_PAYS, data_dir: str = DATA_DIR,
                 classement: str = CLASSEMENT, verbose: bool = True) -> list:
    """Pays des viviers : liste imposée (VIVIER_PAYS), sinon FR + pays de la comparaison ERC
    (erc_meta.pays_compares) + pays de la comparaison bibliométrique (biblio_meta.pays_compares,
    lus dans indicateurs/<classement>_params.json). FR toujours compris (outre-mer : PAYS_FR,
    regroupé sous FR)."""
    if pays is None:
        from .erc import pays_compares_erc
        erc = pays_compares_erc(erc_dir)
        f_ind = Path(data_dir) / "indicateurs" / f"{classement}_params.json"
        p_ind = json.loads(f_ind.read_text()) if f_ind.exists() else {}
        biblio = set(p_ind.get("pays_compares") or []) | set(p_ind.get("pays_effectifs") or [])
        if verbose and not erc:
            print("erc_meta.json absent : pas de pays ERC (lancer compute_erc() avant)")
        if verbose and not biblio:
            print(f"{f_ind} absent : pas de pays de la bibliométrie (lancer compute_indicators() avant)")
        pays = set(erc) | biblio
    return sorted({"FR", *[p.upper() for p in pays]} - set(PAYS_FR) | {"FR"})


# --- 1. Extraction ---------------------------------------------------------------------------

def extraire_auteurs(viviers_dir: str = VIVIERS_DIR, data_dir: str = DATA_DIR,
                     annee_min: int = VIVIER_PUB_ANNEE_MIN, types: tuple = TYPES, pays=None,
                     erc_dir: str = ERC_DIR, workers: int = 8, urls: list = None,
                     verbose: bool = True) -> dict:
    """Signatures des pays lus -> <viviers_dir>/auteurs/ (voir l'en-tête).
    Seule : relit les works pour les viviers uniquement ; publications_auteurs.extraire_publications()
    fait prix et viviers en une lecture.
    pays : défaut pays_viviers() (VIVIER_PAYS, sinon FR + pays ERC + pays bibliométrie)."""
    return extraire_publications(prix=False, viviers=True, viviers_dir=viviers_dir, data_dir=data_dir,
                                 erc_dir=erc_dir, vivier_annee_min=annee_min, pays=pays, types=types,
                                 workers=workers, urls=urls, verbose=verbose)


# --- 2. Calcul -------------------------------------------------------------------------------

def compute_viviers(viviers_dir: str = VIVIERS_DIR, data_dir: str = DATA_DIR,
                    classement: str = CLASSEMENT, perimetre: str = VIVIER_PERIMETRE,
                    annee_debut: int = VIVIER_ANNEE_DEBUT, annee_fin: int = VIVIER_ANNEE_FIN,
                    min_articles: int = VIVIER_MIN_ARTICLES,
                    annees_recentes: int = VIVIER_ANNEES_RECENTES,
                    min_articles_recents: int = VIVIER_MIN_ARTICLES_RECENTS,
                    max_auteurs: int = VIVIER_MAX_AUTEURS_PAR_ARTICLE,
                    part_min_etablissement: float = VIVIER_PART_MIN_ETABLISSEMENT,
                    part_min_pays: float = VIVIER_PART_MIN_PAYS,
                    seuil_fraction_top10: float = VIVIER_SEUIL_FRACTION_TOP10,
                    min_top10: int = VIVIER_MIN_TOP10,
                    min_auteur_principal: int = VIVIER_MIN_AUTEUR_PRINCIPAL,
                    etab_countries=ETAB_COUNTRIES, lineage_cuts=LINEAGE_CUTS, etab_exclude=ETAB_EXCLUDE,
                    out_suffix: str = "", verbose: bool = True) -> dict:
    """Chercheurs actifs et effectifs des viviers (voir l'en-tête).
    out_suffix : ajouté aux noms des sorties (essais d'autres fenêtres ou seuils)."""
    from .indicators import _fichier_top10, _preparer_etablissements

    viviers_dir, data_dir = Path(viviers_dir), Path(data_dir)
    auteurs = viviers_dir / "auteurs"
    if not any(auteurs.glob("*.parquet")):
        raise FileNotFoundError(f"{auteurs} vide : lancer extraire_auteurs()")
    f_top10, params10 = _fichier_top10(data_dir, classement, perimetre)
    f_disc = data_dir / "disciplines" / f"{classement}.parquet"
    f_ent = data_dir / "indicateurs" / f"{classement}_entites.parquet"
    debut, debut_recent = int(annee_debut), int(annee_fin) - int(annees_recentes) + 1
    if debut > int(annee_fin) or debut_recent < debut:
        raise ValueError(f"période {debut}-{annee_fin} incohérente (annees_recentes={annees_recentes})")
    params_extraction = json.loads((auteurs / "params.json").read_text())
    if params_extraction["annee_min"] > debut:
        raise ValueError(f"fenêtre {debut}-{annee_fin} : signatures extraites seulement depuis "
                         f"{params_extraction['annee_min']} (relancer extraire_auteurs(annee_min={debut}))")

    con = connect(str(data_dir))
    # Articles du périmètre dans la fenêtre
    con.execute(f"""
        CREATE TEMP TABLE art AS
        SELECT work_id, annee_publication, discipline_code, fraction_top10_cites
        FROM read_parquet('{f_top10}')
        WHERE annee_publication BETWEEN {debut} AND {int(annee_fin)}
    """)
    # Signatures : une ligne par (chercheur, article) ; un auteur listé deux fois sur un même
    # article (doublon OpenAlex) ne compte qu'une fois
    max_aut = f"s.nb_auteurs <= {int(max_auteurs)}" if max_auteurs else "TRUE"
    con.execute(f"""
        CREATE TEMP TABLE sig_tout AS
        SELECT s.author_id, s.work_id, max(s.nb_auteurs) AS nb_auteurs,
               bool_or(s.position_auteur IN ('first', 'last') OR s.correspondant) AS auteur_principal,
               list_distinct(flatten(list(s.pays))) AS pays,
               list_distinct(flatten(list(coalesce(s.inst_ids, [])))) AS inst_ids,
               any_value(s.nom) AS nom, max(s.orcid) AS orcid
        FROM read_parquet('{auteurs}/*.parquet') s
        WHERE s.work_id IN (SELECT work_id FROM art)
        GROUP BY 1, 2
    """)
    con.execute(f"""
        CREATE TEMP TABLE sig AS
        SELECT s.*, a.annee_publication AS annee, a.discipline_code, a.fraction_top10_cites
        FROM sig_tout s JOIN art a USING (work_id)
        WHERE {max_aut}
    """)

    # Chercheurs : effectifs de la fenêtre, puis filtre d'activité
    con.execute(f"""
        CREATE TEMP TABLE aut AS
        SELECT author_id,
               arg_max(nom, annee) AS nom,
               arg_max(orcid, annee) FILTER (WHERE orcid IS NOT NULL) AS orcid,
               count(*)::INTEGER AS nb_articles,
               count(*) FILTER (WHERE annee >= {debut_recent})::INTEGER AS nb_articles_recents,
               count(*) FILTER (WHERE auteur_principal)::INTEGER AS nb_articles_auteur_principal,
               round(sum(fraction_top10_cites), 4)::DOUBLE AS nb_articles_top10_cites,
               count(*) FILTER (WHERE fraction_top10_cites >= {float(seuil_fraction_top10)})::INTEGER
                   AS nb_articles_top10,
               min(annee)::INTEGER AS annee_premiere_publication,
               max(annee)::INTEGER AS annee_derniere_publication
        FROM sig GROUP BY 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE actifs AS
        SELECT *, nb_articles_top10 >= {int(min_top10)} AS top10,
               nb_articles_auteur_principal >= {int(min_auteur_principal)} AS auteur_principal
        FROM aut
        WHERE nb_articles >= {int(min_articles)} AND nb_articles_recents >= {int(min_articles_recents)}
    """)

    # Rattachement aux pays : part des articles récents avec une affiliation dans le pays
    con.execute(f"""
        CREATE TEMP TABLE ratt_pays AS
        WITH sp AS (
            SELECT s.author_id, s.work_id, unnest(s.pays) AS pays_code
            FROM sig s JOIN actifs a USING (author_id)
            WHERE s.annee >= {debut_recent}
        )
        SELECT sp.author_id, sp.pays_code, count(DISTINCT sp.work_id) AS n,
               count(DISTINCT sp.work_id) / any_value(a.nb_articles_recents) AS part
        FROM sp JOIN actifs a USING (author_id)
        GROUP BY 1, 2
        HAVING count(DISTINCT sp.work_id) / any_value(a.nb_articles_recents) >= {float(part_min_pays)}
    """)

    # Rattachement aux établissements : part des articles récents affiliés à l'établissement
    # (institution ou l'une de ses tutelles, lignée corrigée), parmi les entités de l'étape 6
    _preparer_etablissements(con, data_dir, etab_countries, lineage_cuts, etab_exclude, None, 0, None)
    if f_ent.exists():
        con.execute(f"""CREATE TEMP TABLE etabs AS SELECT entite_id FROM read_parquet('{f_ent}')
                        WHERE entite_type = 'etablissement'""")
    else:
        if verbose:
            print(f"{f_ent} absent : rattachement à tous les ROR français (lancer compute_indicators())")
        con.execute("CREATE TEMP TABLE etabs AS SELECT DISTINCT entite_id FROM cible")
    con.execute(f"""
        CREATE TEMP TABLE ratt AS
        WITH si AS (
            SELECT s.author_id, s.work_id, unnest(s.inst_ids) AS inst_id
            FROM sig s JOIN actifs a USING (author_id)
            WHERE s.annee >= {debut_recent}
        ), se AS (
            SELECT DISTINCT si.author_id, si.work_id, c.entite_id
            FROM si JOIN cible c USING (inst_id)
            WHERE c.entite_id IN (SELECT entite_id FROM etabs)
        )
        SELECT se.author_id, se.entite_id, count(*) AS n, count(*) / any_value(a.nb_articles_recents) AS part
        FROM se JOIN actifs a USING (author_id)
        GROUP BY 1, 2
        HAVING count(*) / any_value(a.nb_articles_recents) >= {float(part_min_etablissement)}
    """)

    # Disciplines : part des articles de la fenêtre par panel
    poids_disciplines(con, "(SELECT s.* FROM sig s JOIN actifs USING (author_id))", "author_id", f_disc)

    # Une ligne par chercheur actif
    chercheurs = con.execute("""
        WITH e AS (SELECT author_id, list(entite_id ORDER BY part DESC, entite_id) AS entites
                   FROM ratt GROUP BY 1),
             pa AS (SELECT author_id, list(pays_code ORDER BY part DESC, pays_code) AS pays
                    FROM ratt_pays GROUP BY 1),
             p AS (SELECT author_id,
                          any_value(discipline_code) FILTER (WHERE principale) AS discipline_code,
                          any_value(domaine_code) FILTER (WHERE principale) AS domaine_code,
                          list({'discipline_code': discipline_code, 'poids': round(poids, 4)}
                               ORDER BY poids DESC, discipline_code) AS disciplines
                   FROM poids GROUP BY 1)
        SELECT a.author_id AS chercheur_id, a.nom, a.orcid, coalesce(pa.pays, []) AS pays,
               coalesce(e.entites, []) AS entites,
               p.discipline_code, p.domaine_code, coalesce(p.disciplines, []) AS disciplines,
               a.nb_articles, a.nb_articles_recents, a.nb_articles_auteur_principal, a.auteur_principal,
               a.nb_articles_top10, a.nb_articles_top10_cites, a.top10,
               a.annee_premiere_publication, a.annee_derniere_publication
        FROM actifs a LEFT JOIN e USING (author_id) LEFT JOIN pa USING (author_id)
        LEFT JOIN p USING (author_id)
        WHERE e.author_id IS NOT NULL OR list_contains(pa.pays, 'FR')
        ORDER BY a.author_id
    """).df()

    # Effectifs : pays et établissements de rattachement
    effectifs = con.execute("""
        WITH ea AS (
            SELECT 'etablissement' AS entite_type, entite_id, author_id FROM ratt
            UNION ALL SELECT 'pays', pays_code, author_id FROM ratt_pays
        )
        SELECT ea.entite_type, ea.entite_id, p.discipline_code,
               count(*) FILTER (WHERE p.principale)::BIGINT AS nb_chercheurs,
               round(sum(p.poids), 4)::DOUBLE AS nb_chercheurs_fractionnaire,
               count(*) FILTER (WHERE p.principale AND a.top10)::BIGINT AS nb_chercheurs_top10,
               round(coalesce(sum(p.poids) FILTER (WHERE a.top10), 0), 4)::DOUBLE
                   AS nb_chercheurs_top10_fractionnaire,
               count(*) FILTER (WHERE p.principale AND a.auteur_principal)::BIGINT
                   AS nb_chercheurs_auteur_principal
        FROM ea JOIN poids p USING (author_id) JOIN actifs a USING (author_id)
        GROUP BY 1, 2, 3
        ORDER BY CASE ea.entite_type WHEN 'pays' THEN 0 ELSE 1 END, 2, 3
    """).df()

    ctrl = con.execute(f"""
        SELECT (SELECT count(DISTINCT author_id) FROM sig_tout) AS nb_auteurs_fenetre,
               (SELECT count(*) FROM sig_tout) AS nb_signatures_fenetre,
               (SELECT count(*) FROM sig_tout WHERE {'FALSE' if not max_auteurs else f'nb_auteurs > {int(max_auteurs)}'})
                   AS nb_signatures_grandes_collaborations,
               (SELECT count(*) FROM aut) AS nb_auteurs_retenus,
               (SELECT count(*) FROM actifs) AS nb_chercheurs_actifs,
               (SELECT count(*) FROM actifs WHERE top10) AS nb_chercheurs_top10,
               (SELECT count(*) FROM actifs WHERE auteur_principal) AS nb_chercheurs_auteur_principal,
               (SELECT count(*) FROM actifs WHERE orcid IS NOT NULL) AS nb_chercheurs_avec_orcid,
               (SELECT count(*) FROM ratt_pays WHERE pays_code = 'FR') AS nb_chercheurs_france,
               (SELECT count(DISTINCT author_id) FROM ratt_pays) AS nb_chercheurs_rattaches_pays,
               (SELECT count(DISTINCT author_id) FROM ratt) AS nb_chercheurs_rattaches,
               (SELECT count(DISTINCT entite_id) FROM ratt) AS nb_etablissements,
               (SELECT count(*) FROM art) AS nb_articles_perimetre,
               (SELECT count(DISTINCT work_id) FROM sig) AS nb_articles_signes_pays_lus,
               (SELECT count(DISTINCT work_id) FROM sig_tout) - (SELECT count(DISTINCT work_id) FROM sig)
                   AS nb_articles_grandes_collaborations
    """).df().iloc[0].to_dict()
    ctrl = {k: int(v) for k, v in ctrl.items()}
    ctrl["part_rattaches"] = round(ctrl["nb_chercheurs_rattaches"] / ctrl["nb_chercheurs_france"], 4) \
        if ctrl["nb_chercheurs_france"] else 0.0
    ctrl["par_pays"] = {r[0]: int(r[1]) for r in con.execute(
        "SELECT pays_code, count(*) FROM ratt_pays GROUP BY 1 ORDER BY 2 DESC").fetchall()}
    ctrl["par_domaine"] = chercheurs["domaine_code"].fillna("(non classé)").value_counts().to_dict()

    viviers_dir.mkdir(parents=True, exist_ok=True)
    for nom, t in (("viviers_chercheurs", chercheurs), ("viviers_effectifs", effectifs)):
        con.register("t", t)
        con.execute(f"COPY t TO '{viviers_dir / (nom + out_suffix + '.parquet')}' "
                    f"(FORMAT parquet, COMPRESSION zstd)")
        con.unregister("t")
    con.close()

    provisoires = []
    W, years = params10.get("citation_window"), params10.get("counts_years") or [None, None]
    if W is not None and years[1] is not None:
        provisoires = [a for a in range(debut, int(annee_fin) + 1) if a + int(W) >= int(years[1])]
    grandes = (f"Les articles à plus de {max_auteurs} auteurs (grandes collaborations) sont ignorés."
               if max_auteurs else "Tous les articles comptent, quel que soit leur nombre d'auteurs.")
    # Publications : description explicite pour les consommateurs (tableaux), d'après les
    # paramètres réellement utilisés pour le fichier top10 (étape 4)
    libelles = {"toutes_revues_openalex": "tous les articles de revues d'OpenAlex",
                "revues_sjr": "articles des revues indexées dans SJR (Scimago, comparable à Scopus)"}
    work_types = params10.get("work_types") or []
    publications = {
        "source": "OpenAlex",
        "snapshot": SNAPSHOT,
        "perimetre": perimetre,
        "perimetre_libelle": libelles.get(perimetre, perimetre),
        "revues_sjr_seulement": perimetre == "revues_sjr",
        "types_documents": work_types,
        "types_sources": params10.get("source_types") or [],
        "annee_debut": debut,
        "annee_fin": int(annee_fin),
        "max_auteurs_par_article": max_auteurs,
        "classement_disciplines": classement,
        "discipline_article": "topic principal OpenAlex -> panel ERC ; articles sans panel exclus",
        "exclusions": ["documents rétractés", "paratexte (éditoriaux, sommaires...)",
                       "sans topic principal ou topic hors classement"]
                      + ([f"plus de {max_auteurs} auteurs (grandes collaborations)"] if max_auteurs else []),
        "signatures_lues": "seules les signatures avec une affiliation dans les pays lus (champ pays) ; "
                           "les articles d'un chercheur publiés depuis un autre pays ne comptent pas",
        "meme_base_que": f"indicateurs bibliométriques du périmètre {perimetre} (effectifs d'articles, "
                         "top 10 %) : mêmes articles, mêmes disciplines",
        "top10": {
            "role": "indicateur (chercheurs top 10 %), pas un filtre : le vivier compte tous les "
                    "chercheurs actifs, top 10 % ou non",
            "part": params10.get("top_share"),
            "reference": "articles les plus cités de la même discipline et de la même année, dans le "
                         "périmètre",
            "fenetre_citation_annees": params10.get("citation_window"),
            "non_cites_exclus": params10.get("exclude_uncited"),
            "seuil_fraction_article": seuil_fraction_top10,
            "min_articles_top10_chercheur": int(min_top10),
            "annees_provisoires": provisoires,
        },
        "nb_articles_perimetre": ctrl["nb_articles_perimetre"],
        "nb_articles_signes_pays_lus": ctrl["nb_articles_signes_pays_lus"],
    }
    types_txt = " et ".join({"article": "articles", "review": "revues de littérature"}.get(t, t)
                            for t in work_types) or "tous types de documents"
    meta = {
        "date_calcul": dt.date.today().isoformat(),
        "classement": classement,
        "perimetre": perimetre,
        "publications": publications,
        "pays": params_extraction.get("pays") or ["FR"],
        "periode": {"annee_debut": debut, "annee_fin": int(annee_fin), "annee_debut_recente": debut_recent},
        "annees_influence_provisoire": provisoires,
        "parametres": {
            "annee_debut": debut, "annee_fin": int(annee_fin), "min_articles": int(min_articles),
            "annees_recentes": int(annees_recentes), "min_articles_recents": int(min_articles_recents),
            "max_auteurs_par_article": max_auteurs, "part_min_etablissement": part_min_etablissement,
            "part_min_pays": part_min_pays,
            "seuil_fraction_top10": seuil_fraction_top10, "min_top10": int(min_top10),
            "min_auteur_principal": int(min_auteur_principal),
            "extraction": params_extraction,
        },
        "controles": ctrl,
        "methode": [
            f"Publications : {publications['perimetre_libelle']} ({types_txt}, {debut}-{annee_fin}, "
            f"snapshot OpenAlex {SNAPSHOT}), chacune classée dans un panel ERC par son topic principal ; "
            "documents rétractés et paratexte exclus. Mêmes articles que les indicateurs "
            "bibliométriques de ce périmètre. Toutes les publications du périmètre comptent, pas "
            "seulement celles du top 10 %.",
            "Vivier : auteurs OpenAlex qui signent avec une affiliation en France ou dans un pays des "
            "comparaisons ERC et bibliométrique. Seules les signatures dans ces pays sont lues ; la "
            "qualité dépend de la désambiguïsation des auteurs par OpenAlex. Ordres de grandeur, pas "
            "des effectifs RH.",
            f"Chercheur actif : au moins {min_articles} articles sur {debut}-{annee_fin}, dont au moins "
            f"{min_articles_recents} sur {debut_recent}-{annee_fin}. {grandes}",
            f"Pays : un chercheur compte pour un pays si au moins {round(100 * part_min_pays)} % de ses "
            f"articles {debut_recent}-{annee_fin} y ont une affiliation ; même règle pour tous les pays.",
            f"Établissement : un chercheur compte pour un établissement si au moins "
            f"{round(100 * part_min_etablissement)} % de ses articles {debut_recent}-{annee_fin} y sont "
            "affiliés (structure ou l'une de ses tutelles : un chercheur d'UMR compte pour l'UMR, ses "
            "universités et organismes). Il peut donc compter pour plusieurs établissements.",
            "Discipline : panel ERC le plus fréquent dans ses articles de la période (effectifs "
            "fractionnaires : part de chaque panel).",
            f"Chercheur « top 10 % » : au moins {min_top10} article(s) parmi les 10 % les plus cités de "
            f"sa discipline et de son année (fraction dans le top au moins {seuil_fraction_top10})"
            + (f" ; années {', '.join(map(str, provisoires))} : fenêtre de citation pas encore close."
               if provisoires else "."),
            "Auteur principal : premier ou dernier auteur, ou auteur correspondant, d'au moins "
            f"{min_auteur_principal} article(s) de la période.",
        ],
    }
    meta["pays_noms"] = {p: PAYS_NOMS.get(p, p) for p in meta["pays"]}
    meta["lecture"] = [
        "Non additif entre établissements : un chercheur d'UMR compte pour l'UMR, ses universités et "
        "ses organismes ; un chercheur peut aussi compter pour plusieurs établissements.",
        "Non additif entre pays : un chercheur partagé (au moins 50 % de ses articles récents dans "
        "chacun de deux pays) compte pour les deux.",
        "La somme des établissements n'est pas la France : les chercheurs français sans établissement "
        "atteignant le seuil comptent pour la France seulement.",
        "Pas d'année : le vivier porte sur toute la période ; rapporter des candidatures ou lauréats "
        "de la même période, divisés par le nombre d'années pour un taux annuel.",
        "Discipline principale (nb_chercheurs) et fractionnaire (nb_chercheurs_fractionnaire) ne se "
        "mélangent pas ; un domaine (LS, PE, SH) = somme de ses panels.",
    ]
    meta["limites"] = [
        "Seuls les chercheurs qui publient des articles de revues sont vus : pas de personnels "
        "sans publication, ni d'effectifs RH.",
        "Sciences humaines et sociales sous-estimées : livres, chapitres et revues peu couvertes par "
        "OpenAlex ne comptent pas ; les taux (candidatures ou lauréats / vivier) y sont donc "
        "surestimés et à comparer avec prudence aux autres domaines.",
        "Désambiguïsation OpenAlex : un chercheur scindé en plusieurs identifiants peut compter "
        "plusieurs fois ou passer sous le seuil d'activité ; deux homonymes fusionnés comptent une fois.",
        "Affiliations : une affiliation absente dans OpenAlex fait perdre la signature ; la "
        "couverture varie selon les pays et les établissements.",
        "Pas d'âge académique : le vivier ne distingue pas les étapes de carrière (ERC Starting, "
        "Consolidator, Advanced).",
        "Pays hors ERC (ex. États-Unis, Chine) : vivier utile pour comparer des tailles ou des parts "
        "top 10 %, pas pour un taux de candidature ERC.",
    ]
    (viviers_dir / f"viviers_meta{out_suffix}.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")
    if verbose:
        c = ctrl
        print(f"Viviers {debut}-{annee_fin} : {c['nb_chercheurs_actifs']:,} chercheurs actifs "
              f"(sur {c['nb_auteurs_retenus']:,} auteurs), {len(c['par_pays'])} pays ; "
              f"France : {c['nb_chercheurs_france']:,}, dont {c['part_rattaches']:.1%} rattachés à un "
              f"établissement ({c['nb_etablissements']:,})")
        print(f"  par pays : {c['par_pays']}")
        print(f"  domaines : {c['par_domaine']}")
    return meta
