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

Deux étapes, après l'étape 4 (top10/) et de préférence l'étape 6 (indicateurs/<nomenclature>_entites) :

1. extraire_auteurs()   (long : relit les works du snapshot sur S3, de l'ordre de l'étape 1 ;
   reprise possible, fichier par fichier)
   Une ligne par (article, auteur) dont l'auteur a sur l'article une institution d'un des pays
   lus. Mêmes filtres que l'étape 1 (types VIVIER_PUB_TYPES : articles, reviews, livres et
   chapitres ; ni rétracté ni paratexte, avec un
   topic), depuis VIVIER_PUB_ANNEE_MIN. Écrit viviers/auteurs/<fichier>.parquet :
     work_id, year, type, topic_id, nb_auteurs, author_id, orcid, nom, position_auteur, correspondant,
     pays (pays lus de ses institutions, outre-mer sous FR),
     inst_ids (toutes ses institutions sur l'article ; seulement si FR est dans pays, sinon null)
   Si la liste des pays ou des types change, tout est relu.
   Lecture partagée avec les prix : publications_auteurs.extraire_publications() écrit les
   deux sorties en une seule lecture des works (voir pipeline, étape "publications").

2. compute_viviers()    (quelques minutes, local)
   Articles retenus : ceux du fichier top10 du périmètre VIVIER_PERIMETRE (articles de revues,
   avec discipline et fraction_top10_cites), publiés dans la fenêtre
   [VIVIER_ANNEE_DEBUT, VIVIER_ANNEE_FIN], à au plus
   VIVIER_MAX_AUTEURS_PAR_ARTICLE auteurs.
   Livres et chapitres (VIVIER_LIVRES_TYPES) dont le panel est dans VIVIER_LIVRES_DOMAINES (SHS
   par défaut) s'ajoutent comme publications ; jamais top 10 %.
   Chercheur actif : au moins VIVIER_MIN_PUBLICATIONS publications dans la fenêtre, dont au moins
   VIVIER_MIN_PUBLICATIONS_RECENTES dans les VIVIER_ANNEES_RECENTES dernières années.
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

from .common import DOMAINES_NOMS, PAYS_NOMS, connect
from .config import (NOMENCLATURE_DISCIPLINAIRE, DATA_DIR, ERC_DIR, ETAB_COUNTRIES, ETAB_EXCLUDE, LINEAGE_CUTS, PAYS_FR, SNAPSHOT, TOP_SHARE,
                     VIVIER_ANNEE_FIN, VIVIER_PART_MIN_PAYS, VIVIER_PAYS, VIVIER_ANNEES_RECENTES, VIVIER_ANNEE_DEBUT,
                     VIVIER_LIVRES_DOMAINES, VIVIER_LIVRES_TYPES, VIVIER_MAX_AUTEURS_PAR_ARTICLE, VIVIER_MIN_PUBLICATIONS,
                     VIVIER_MIN_PUBLICATIONS_RECENTES, VIVIER_PUB_TYPES,
                     VIVIER_MIN_AUTEUR_PRINCIPAL, VIVIER_MIN_TOP10, VIVIER_PART_MIN_ETABLISSEMENT,
                     VIVIER_PERIMETRE, VIVIER_PUB_ANNEE_MIN, VIVIER_SEUIL_FRACTION_TOP10, VIVIERS_DIR)
from .publications_auteurs import extraire_publications, poids_disciplines
from .disciplines import disciplines_path


def pays_viviers(erc_dir: str = ERC_DIR, pays=VIVIER_PAYS, data_dir: str = DATA_DIR,
                 nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, verbose: bool = True) -> list:
    """Pays des viviers : liste imposée (VIVIER_PAYS), sinon FR + pays de la comparaison ERC
    (erc_meta.pays_compares) + pays de la comparaison bibliométrique (biblio_meta.pays_compares,
    lus dans indicateurs/<nomenclature>_params.json). FR toujours compris (outre-mer : PAYS_FR,
    regroupé sous FR)."""
    if pays is None:
        from .erc import pays_compares_erc
        erc = pays_compares_erc(erc_dir)
        f_ind = Path(data_dir) / "indicateurs" / f"{nomenclature}_params.json"
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
                     annee_min: int = VIVIER_PUB_ANNEE_MIN, types: tuple = VIVIER_PUB_TYPES, pays=None,
                     erc_dir: str = ERC_DIR, workers: int = 8, urls: list = None,
                     verbose: bool = True) -> dict:
    """Signatures des pays lus -> <viviers_dir>/auteurs/ (voir l'en-tête).
    Seule : relit les works pour les viviers uniquement ; publications_auteurs.extraire_publications()
    fait prix et viviers en une lecture.
    pays : défaut pays_viviers() (VIVIER_PAYS, sinon FR + pays ERC + pays bibliométrie)."""
    return extraire_publications(prix=False, viviers=True, viviers_dir=viviers_dir, data_dir=data_dir,
                                 erc_dir=erc_dir, vivier_annee_min=annee_min, pays=pays, vivier_types=types,
                                 workers=workers, urls=urls, verbose=verbose)


# --- Textes publics de viviers_meta ----------------------------------------------------------
# methode, lecture, limites et limites_par_usage sont affichés tels quels au public par
# tableaux : pas de nom de champ, phrases courtes, chiffres tirés des paramètres du calcul.

MOIS = ("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
        "octobre", "novembre", "décembre")

# Quand afficher chaque groupe de limites_par_usage (pour les développeurs de tableaux)
LIMITES_USAGES = {
    "sciences_humaines_sociales": "chiffres du domaine SH, ou toutes disciplines confondues",
    "types_bourse_erc": "taux par type de bourse ERC (Starting, Consolidator, Advanced)",
    "pays_sans_comparaison_erc": "pays listés dans pays_sans_comparaison_erc (pas de taux de candidature ERC)",
}


def _pct(x) -> str:
    return f"{round(100 * float(x))} %"


def _nb(n, singulier: str, pluriel: str = None) -> str:
    n = int(n)
    return f"{n} {singulier if n <= 1 else (pluriel or singulier + 's')}"


def _annees(a, b) -> str:
    a, b = int(a), int(b)
    return f"en {b}" if a == b else (f"en {a} ou {b}" if b == a + 1 else f"de {a} à {b}")


def _liste(mots) -> str:
    mots = [str(m) for m in mots]
    return ", ".join(mots[:-1]) + " et " + mots[-1] if len(mots) > 1 else "".join(mots)


def _date(iso: str) -> str:
    try:
        d = dt.date.fromisoformat(str(iso))
    except ValueError:
        return str(iso)
    return f"{'1er' if d.day == 1 else d.day} {MOIS[d.month - 1]} {d.year}"


def _textes_publics(*, perimetre, types, debut, fin, debut_recent, min_publications,
                    min_publications_recentes, max_auteurs, part_min_pays, part_min_etablissement,
                    top_share, seuil_fraction_top10, min_top10, min_auteur_principal, provisoires,
                    livres_types, livres_domaines) -> dict:
    """Textes de viviers_meta pour le public : methode, lecture, limites (générales) et
    limites_par_usage (à n'afficher que dans le contexte indiqué par LIMITES_USAGES).
    Une entrée = un paragraphe de phrases courtes."""
    periode, recent = _annees(debut, fin), _annees(debut_recent, fin)
    top = _pct(top_share)
    revues = {"toutes_revues_openalex": "les articles de toutes les revues indexées dans OpenAlex",
              "revues_sjr": "les articles des revues indexées dans SJR (Scimago)"}.get(
        perimetre, "les articles de revues")
    if "review" in types:
        revues += ", y compris les revues de littérature"
    domaines = _liste(DOMAINES_NOMS.get(d, d).lower() for d in livres_domaines)
    livres = _liste({"book": "livres", "book-chapter": "chapitres d'ouvrages"}.get(t, t) for t in livres_types)
    p = lambda *phrases: " ".join(x for x in phrases if x)  # noqa: E731

    methode = [
        p(f"Publications : {revues}, parus {periode}, d'après la base ouverte OpenAlex "
          f"(version du {_date(SNAPSHOT)}).",
          "Ce sont les mêmes articles que pour les indicateurs bibliométriques.",
          f"En {domaines}, les {livres} comptent aussi." if livres_types else "",
          "Dans les autres domaines, ils ne comptent pas : beaucoup d'actes de conférence y sont classés."
          if livres_types else "",
          "Les publications rétractées, les éditoriaux et les sommaires ne comptent pas.",
          "Toutes les publications comptent, pas seulement les plus citées.",
          "Chaque publication est classée dans un panel ERC d'après son thème principal."),
        p("Vivier : chercheurs qui signent des publications depuis la France ou depuis l'un des pays comparés.",
          "Seules les publications signées depuis ces pays sont prises en compte.",
          "Ce sont des ordres de grandeur, pas des effectifs de personnel."),
        p(f"Chercheur actif : au moins {_nb(min_publications, 'publication')} {periode}, "
          f"dont au moins {_nb(min_publications_recentes, 'publication')} {recent}.",
          f"Les publications de plus de {max_auteurs} auteurs ne comptent pas (grandes collaborations)."
          if max_auteurs else ""),
        p(f"Pays : un chercheur compte pour un pays si au moins {_pct(part_min_pays)} de ses "
          f"publications {recent} y sont signées.",
          "La règle est la même pour tous les pays."),
        p(f"Établissement : un chercheur compte pour un établissement si au moins "
          f"{_pct(part_min_etablissement)} de ses publications {recent} y sont signées.",
          "Un chercheur d'unité mixte compte pour l'unité, ses universités et ses organismes."),
        p("Discipline : chaque chercheur est classé dans le panel ERC où il publie le plus.",
          "Un autre décompte le répartit entre ses panels, en proportion de ses publications."),
        p(f"Chercheur « top {top} » : au moins {_nb(min_top10, 'article')} parmi les {top} les plus "
          "cités de sa discipline et de son année.",
          "Des articles à égalité de citations à la limite se partagent les places restantes."
          if seuil_fraction_top10 else "",
          f"Un tel article n'est retenu que si sa part atteint au moins {_pct(seuil_fraction_top10)}."
          if seuil_fraction_top10 else "",
          f"Pour {_liste(provisoires)}, les citations ne sont pas encore complètes." if provisoires else ""),
        p("Auteur principal : premier ou dernier auteur, ou auteur de correspondance, "
          f"d'au moins {_nb(min_auteur_principal, 'publication')} {periode}."),
    ]

    lecture = [
        p("Un chercheur peut compter pour plusieurs établissements.",
          "Les chiffres des établissements ne s'additionnent donc pas."),
        p("Le total des établissements n'est pas celui de la France.",
          "Certains chercheurs en France ne sont rattachés à aucun établissement."),
        p(f"Le vivier couvre toute la période {debut}-{fin}.", "Il n'y a pas de chiffre par année."),
    ]
    if float(part_min_pays) <= 0.5:
        lecture.insert(1, p(
            f"Un chercheur partagé entre deux pays compte pour chacun s'il y signe au moins "
            f"{_pct(part_min_pays)} de ses publications récentes.",
            "Les chiffres des pays ne s'additionnent donc pas."))

    limites = [
        p(f"Seuls les chercheurs qui publient sont comptés : articles de revues, et {livres} en {domaines}."
          if livres_types else "Seuls les chercheurs qui publient des articles de revues sont comptés.",
          "Le vivier ne reflète pas les effectifs de personnel."),
        p("OpenAlex attribue lui-même chaque publication à un auteur.",
          "Un chercheur scindé en plusieurs profils peut compter plusieurs fois, ou ne pas atteindre "
          "le seuil d'activité.",
          "Deux homonymes confondus ne comptent qu'une fois."),
        p("Une publication dont l'affiliation manque dans OpenAlex n'est pas prise en compte.",
          "La qualité des affiliations varie selon les pays et les établissements."),
    ]
    par_usage = {
        "sciences_humaines_sociales": [p(
            "OpenAlex couvre mal une partie des ouvrages et des revues de sciences humaines et "
            "sociales, notamment francophones.",
            "Les livres et chapitres comptent, mais le vivier y reste sans doute sous-estimé."
            if livres_types else "Les livres et chapitres ne comptent pas : le vivier y est sous-estimé.",
            "Les taux rapportés au vivier y sont donc surestimés.",
            "La comparaison avec les autres domaines demande de la prudence.")],
        "types_bourse_erc": [p(
            "Le vivier ne distingue pas les étapes de carrière.",
            "Il ne correspond pas exactement aux publics des bourses Starting, Consolidator et Advanced.")],
        "pays_sans_comparaison_erc": [p(
            "Ce pays ne fait pas partie de la comparaison des candidatures ERC.",
            "Son vivier sert à comparer des tailles ou des parts de chercheurs très cités, "
            "pas un taux de candidature.")],
    }
    return {"methode": methode, "lecture": lecture, "limites": limites, "limites_par_usage": par_usage}


# --- 2. Calcul -------------------------------------------------------------------------------

def compute_viviers(viviers_dir: str = VIVIERS_DIR, data_dir: str = DATA_DIR,
                    nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, perimetre: str = VIVIER_PERIMETRE,
                    annee_debut: int = VIVIER_ANNEE_DEBUT, annee_fin: int = VIVIER_ANNEE_FIN,
                    min_publications: int = VIVIER_MIN_PUBLICATIONS,
                    annees_recentes: int = VIVIER_ANNEES_RECENTES,
                    min_publications_recentes: int = VIVIER_MIN_PUBLICATIONS_RECENTES,
                    max_auteurs: int = VIVIER_MAX_AUTEURS_PAR_ARTICLE,
                    part_min_etablissement: float = VIVIER_PART_MIN_ETABLISSEMENT,
                    part_min_pays: float = VIVIER_PART_MIN_PAYS,
                    seuil_fraction_top10: float = VIVIER_SEUIL_FRACTION_TOP10,
                    min_top10: int = VIVIER_MIN_TOP10,
                    min_auteur_principal: int = VIVIER_MIN_AUTEUR_PRINCIPAL,
                    livres_types=VIVIER_LIVRES_TYPES, livres_domaines=VIVIER_LIVRES_DOMAINES,
                    etab_countries=ETAB_COUNTRIES, lineage_cuts=LINEAGE_CUTS, etab_exclude=ETAB_EXCLUDE,
                    erc_dir: str = ERC_DIR, out_suffix: str = "", verbose: bool = True) -> dict:
    """Chercheurs actifs et effectifs des viviers (voir l'en-tête).
    erc_dir : pays de la comparaison ERC (pour pays_sans_comparaison_erc).
    out_suffix : ajouté aux noms des sorties (essais d'autres fenêtres ou seuils)."""
    from .indicators import _fichier_top10, _preparer_etablissements

    viviers_dir, data_dir = Path(viviers_dir), Path(data_dir)
    auteurs = viviers_dir / "auteurs"
    if not any(auteurs.glob("*.parquet")):
        raise FileNotFoundError(f"{auteurs} vide : lancer extraire_auteurs()")
    f_top10, params10 = _fichier_top10(data_dir, nomenclature, perimetre)
    f_disc = disciplines_path(nomenclature, data_dir)
    f_ent = data_dir / "indicateurs" / f"{nomenclature}_entites.parquet"
    debut, debut_recent = int(annee_debut), int(annee_fin) - int(annees_recentes) + 1
    if debut > int(annee_fin) or debut_recent < debut:
        raise ValueError(f"période {debut}-{annee_fin} incohérente (annees_recentes={annees_recentes})")
    params_extraction = json.loads((auteurs / "params.json").read_text())
    if params_extraction["annee_min"] > debut:
        raise ValueError(f"fenêtre {debut}-{annee_fin} : signatures extraites seulement depuis "
                         f"{params_extraction['annee_min']} (relancer extraire_auteurs(annee_min={debut}))")
    livres_types = list(livres_types or []) if livres_domaines else []
    manque = set(livres_types) - set(params_extraction.get("types") or [])
    if manque or params_extraction.get("version", 1) < 2:
        raise ValueError(f"extraction ancienne (types {sorted(manque)} absents ou colonnes manquantes) : "
                         f"relancer extraire_publications()")

    con = connect(str(data_dir))
    # Publications retenues dans la période : articles du périmètre (fichier top10, avec la
    # fraction top 10 %), plus livres et chapitres dont le panel est dans livres_domaines
    # (topic principal -> panel, même table ; pas de fraction top 10 %)
    livres_sql = ""
    if livres_types:
        livres_sql = f"""
        UNION ALL
        SELECT s.work_id, any_value(s.year)::SMALLINT, any_value(d.discipline_code), NULL::DOUBLE, 'livre'
        FROM read_parquet('{auteurs}/*.parquet') s
        JOIN read_parquet('{f_disc}') d ON upper(s.topic_id) = d.topic_id
        WHERE s.type IN ({", ".join(f"'{t}'" for t in livres_types)})
          AND s.year BETWEEN {debut} AND {int(annee_fin)}
          AND d.discipline_code IS NOT NULL
          AND d.domaine_code IN ({", ".join(f"'{x}'" for x in livres_domaines)})
        GROUP BY s.work_id"""
    con.execute(f"""
        CREATE TEMP TABLE art AS
        SELECT work_id, annee_publication, discipline_code, fraction_top10_cites, 'article' AS nature
        FROM read_parquet('{f_top10}')
        WHERE annee_publication BETWEEN {debut} AND {int(annee_fin)}
        {livres_sql}
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
        SELECT s.*, a.annee_publication AS annee, a.discipline_code, a.fraction_top10_cites, a.nature
        FROM sig_tout s JOIN art a USING (work_id)
        WHERE {max_aut}
    """)

    # Chercheurs : effectifs de la fenêtre, puis filtre d'activité
    con.execute(f"""
        CREATE TEMP TABLE aut AS
        SELECT author_id,
               arg_max(nom, annee) AS nom,
               arg_max(orcid, annee) FILTER (WHERE orcid IS NOT NULL) AS orcid,
               count(*)::INTEGER AS nb_publications,
               count(*) FILTER (WHERE annee >= {debut_recent})::INTEGER AS nb_publications_recentes,
               count(*) FILTER (WHERE nature = 'livre')::INTEGER AS nb_livres_chapitres,
               count(*) FILTER (WHERE auteur_principal)::INTEGER AS nb_publications_auteur_principal,
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
               nb_publications_auteur_principal >= {int(min_auteur_principal)} AS auteur_principal
        FROM aut
        WHERE nb_publications >= {int(min_publications)} AND nb_publications_recentes >= {int(min_publications_recentes)}
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
               count(DISTINCT sp.work_id) / any_value(a.nb_publications_recentes) AS part
        FROM sp JOIN actifs a USING (author_id)
        GROUP BY 1, 2
        HAVING count(DISTINCT sp.work_id) / any_value(a.nb_publications_recentes) >= {float(part_min_pays)}
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
        SELECT se.author_id, se.entite_id, count(*) AS n, count(*) / any_value(a.nb_publications_recentes) AS part
        FROM se JOIN actifs a USING (author_id)
        GROUP BY 1, 2
        HAVING count(*) / any_value(a.nb_publications_recentes) >= {float(part_min_etablissement)}
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
               a.nb_publications, a.nb_publications_recentes, a.nb_livres_chapitres,
               a.nb_publications_auteur_principal, a.auteur_principal,
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
               (SELECT count(*) FROM art WHERE nature = 'article') AS nb_articles_perimetre,
               (SELECT count(*) FROM art WHERE nature = 'livre') AS nb_livres_chapitres,
               (SELECT count(*) FROM actifs WHERE nb_livres_chapitres > 0) AS nb_chercheurs_avec_livres,
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
    noms_types = {"book": "livres", "book-chapter": "chapitres d'ouvrages"}
    livres_txt = " et ".join(noms_types.get(t, t) for t in livres_types)
    domaines_txt = ", ".join(livres_domaines or [])
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
        "nomenclature_disciplinaire": nomenclature,
        "discipline_article": "topic principal OpenAlex -> panel ERC ; articles sans panel exclus",
        "exclusions": ["documents rétractés", "paratexte (éditoriaux, sommaires...)",
                       "sans topic principal ou topic hors nomenclature"]
                      + ([f"plus de {max_auteurs} auteurs (grandes collaborations)"] if max_auteurs else []),
        "signatures_lues": "seules les signatures avec une affiliation dans les pays lus (champ pays) ; "
                           "les articles d'un chercheur publiés depuis un autre pays ne comptent pas",
        "meme_base_que": f"indicateurs bibliométriques du périmètre {perimetre} (effectifs d'articles, "
                         "top 10 %) : mêmes articles, mêmes disciplines"
                         + (" ; les livres et chapitres s'y ajoutent (hors bibliométrie)" if livres_types else ""),
        "livres_chapitres": {
            "inclus": bool(livres_types),
            "types_documents": livres_types,
            "domaines": list(livres_domaines or []) if livres_types else [],
            "role": "comptent comme des publications (activité, discipline, rattachements aux pays et "
                    "établissements), une publication chacun ; jamais « top 10 % » (pas de référence "
                    "de citations)",
            "pourquoi_shs_seulement": "hors SHS, OpenAlex classe beaucoup d'actes de conférence en "
                                      "chapitres (ex. Springer LNCS en informatique), ce qui gonflerait "
                                      "les viviers des autres domaines",
            "discipline": "topic principal OpenAlex -> panel ERC, même table que les articles",
            "nb_periode": ctrl["nb_livres_chapitres"],
            "nb_chercheurs_actifs_avec_livres": ctrl["nb_chercheurs_avec_livres"],
        },
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
    if livres_types:
        publications["perimetre_libelle"] += (f", plus les {livres_txt} des domaines {domaines_txt} "
                                              "(sciences humaines et sociales)")
    textes = _textes_publics(
        perimetre=perimetre, types=work_types, debut=debut, fin=int(annee_fin), debut_recent=debut_recent,
        min_publications=min_publications, min_publications_recentes=min_publications_recentes,
        max_auteurs=max_auteurs, part_min_pays=part_min_pays, part_min_etablissement=part_min_etablissement,
        top_share=params10.get("top_share") or TOP_SHARE, seuil_fraction_top10=seuil_fraction_top10,
        min_top10=min_top10, min_auteur_principal=min_auteur_principal, provisoires=provisoires,
        livres_types=livres_types, livres_domaines=list(livres_domaines or []))
    meta = {
        "date_calcul": dt.date.today().isoformat(),
        "nomenclature_disciplinaire": nomenclature,
        "perimetre": perimetre,
        "publications": publications,
        "pays": params_extraction.get("pays") or ["FR"],
        "periode": {"annee_debut": debut, "annee_fin": int(annee_fin), "annee_debut_recente": debut_recent},
        "annees_influence_provisoire": provisoires,
        "parametres": {
            "annee_debut": debut, "annee_fin": int(annee_fin), "min_publications": int(min_publications),
            "annees_recentes": int(annees_recentes), "min_publications_recentes": int(min_publications_recentes),
            "max_auteurs_par_article": max_auteurs, "part_min_etablissement": part_min_etablissement,
            "part_min_pays": part_min_pays,
            "seuil_fraction_top10": seuil_fraction_top10, "min_top10": int(min_top10),
            "min_auteur_principal": int(min_auteur_principal),
            "livres_types": livres_types, "livres_domaines": list(livres_domaines or []),
            "extraction": params_extraction,
        },
        "controles": ctrl,
        "methode": textes["methode"],
    }
    meta["pays_noms"] = {p: PAYS_NOMS.get(p, p) for p in meta["pays"]}
    from .erc import pays_compares_erc
    meta["pays_sans_comparaison_erc"] = sorted(set(meta["pays"]) - set(pays_compares_erc(erc_dir)))
    meta["lecture"] = textes["lecture"]
    meta["limites"] = textes["limites"]
    meta["limites_par_usage"] = textes["limites_par_usage"]
    meta["limites_usages"] = {k: v for k, v in LIMITES_USAGES.items() if k in textes["limites_par_usage"]}
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
