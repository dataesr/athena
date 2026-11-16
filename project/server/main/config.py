"""
config.py
=========
Paramètres de la méthode, regroupés en un seul endroit.

Règle de nommage des données
----------------------------
  - Étapes 1 et 2 (extraction) : noms d'OpenAlex, en anglais (work_id, year,
    topic_id, inst_id, ror...), pour rester calqués sur la source.
  - À partir de l'étape 3 : noms français explicites, identiques dans tous les
    fichiers, la base Mongo et le front (voir CONTRAT_DONNEES.md) :
      classement, perimetre, annee_publication, discipline_code, entite_id,
      nb_articles, nb_articles_top10_cites, nb_articles_revues_tete_disciplinaires,
      nb_articles_revues_tete_multidisciplinaires, influence_provisoire...
    Préfixe nb_ = un nombre d'articles ; part_ = un ratio entre 0 et 1.

Chaque fonction des étapes 3 à 7 prend ces valeurs comme valeurs par défaut :
  - pour changer un choix partout : modifier ce fichier ;
  - pour un essai ponctuel : passer l'argument à l'appel,
    ex. compute_top10(work_types=("article", "review")).

Les chemins relatifs sont résolus depuis le répertoire courant
(/src dans le conteneur).
"""

import os

# --- Dossiers et fichiers d'entrée ----------------------------------------------
# Choix du snapshot OpenAlex utilisé par TOUTES les étapes :
#   SNAPSHOT = None          -> /data (données extraites avant la gestion des snapshots)
#   SNAPSHOT = "2026-10-01"  -> /data/snapshots/2026-10-01 (créé par extract_snapshot())
# Liste des snapshots disponibles localement : extract_works.list_snapshots()
DATA_ROOT = "/data"
SNAPSHOT = "2026-11-04"            # figé sur le snapshot quotidien de cette date

# Source des snapshots OpenAlex :
#   "public" : bucket public s3://openalex, snapshot trimestriel, seul le dernier
#   "daily"  : offre payante, s3://openalex-snapshots/full/<date>/parquet/, un
#              snapshot complet par jour ; clé API lue dans la variable
#              d'environnement OPENALEX_API_KEY ; besoin de boto3 (pour lister les fichiers)
SNAPSHOT_SOURCE = "daily"
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")   # ne jamais écrire la clé en dur ici
DAILY_BUCKET = "openalex-snapshots"
DAILY_PREFIX = "full"
S3_REGION = "us-east-1"        # région du bucket ; à ajuster si DuckDB renvoie une erreur 301
DATA_DIR = f"{DATA_ROOT}/snapshots/{SNAPSHOT}" if SNAPSHOT else DATA_ROOT
TOPICS_PATH = "topics/openalex_topics_erc_panels.json"
SJR_PATH = "scimago/scimagojr_2025.csv"

# --- Classement (étape 3) -------------------------------------------------------
# Nom du classement topic -> discipline : écrit dans <DATA_DIR>/disciplines/<classement>.parquet.
# Plusieurs classements peuvent coexister (erc, erc_alt, ost, ...).
CLASSEMENT = "erc"

# --- Périmètre des indicateurs (étapes 4 à 6) -----------------------------------
# Ne sert PAS au classement : il définit quels articles composent « le monde ».
WORK_TYPES = ("article",)          # ("article", "review") pour inclure les reviews
SOURCE_TYPES = ("journal",)        # None = toutes les sources
ANNEE_MIN = 2015
ANNEE_MAX = 2025                   # 2026 incomplète ; quelques dates aberrantes (2027+)
# Périmètre de référence (« le monde » au dénominateur des parts) :
#   "toutes_revues_openalex" : tous les articles de revues d'OpenAlex
#   "revues_sjr"             : seulement les revues indexées dans SJR (comparable
#                              à Scopus) ; nécessite l'étape 5 (revues_tete/sources_sjr.parquet)
PERIMETRE = "toutes_revues_openalex"

# --- Influence : top 10 % des plus cités (étape 4) -------------------------------
CITATION_WINDOW = 3     # citations de l'année de publication + N années suivantes ;
                        # None = cited_by_count cumulé (pas de fenêtre fixe)
TOP_SHARE = 0.10
TOP_EXCLUDE_UNCITED = True   # un article jamais cité n'entre jamais dans le top
                             # (sinon, dans une cellule à > 90 % de non-cités,
                             #  les non-cités se partagent le reste du top)
COUNTS_FIRST_YEAR = None     # 1re année couverte par counts_by_year (OpenAlex ne garde
                             # que ~10 ans) ; None = détection automatique.
                             # Les cohortes dont la fenêtre commence avant basculent
                             # sur cited_by_count cumulé.

# --- Prestige : revues de tête (étape 5) -----------------------------------------
TOP_JOURNALS_N = 10
JOURNAL_MIN_ARTICLES = 50     # nb minimal d'articles de la discipline dans la revue
JOURNAL_MIN_SHARE = 0.25      # part minimale des articles de la revue dans la discipline
SJR_MODE = "single"           # "single" : une liste (SJR_PATH) appliquée à toutes les années
SJR_TYPES = ("journal",)     # types SJR retenus (journal, book series, conference and proceedings...)
JOURNAL_REF_YEARS = None     # (début, fin) : années servant à rattacher une revue à une
                             # discipline ; None = toutes les années du périmètre
# Exclusion des revues de synthèse (leur SJR est gonflé par les reviews) :
JOURNAL_MAX_REVIEW_SHARE = 0.5   # part maximale de « review » parmi article+review ; None = pas de filtre
JOURNAL_REVIEW_TITLE_REGEX = (r"\bReviews\b|^Annual Review|^Trends in |^Current Opinion|^Progress in "
                              r"|\bSurveys\b")
                                 # titres SJR de revues de synthèse ; None = pas de filtre
JOURNAL_MAX_REFS_PER_DOC = None  # ex. 150 : exclut les revues à plus de N références par
                                 # document (colonne SJR « Ref. / Doc. », typique des
                                 # synthèses) ; None = pas de filtre. Voir la colonne
                                 # refs_par_doc du CSV de relecture avant de l'activer.
# Revues multidisciplinaires (Nature, Science, PNAS...) ajoutées aux revues de tête
# de TOUTES les disciplines (en plus des TOP_JOURNALS_N revues propres).
# Elles n'atteignent jamais JOURNAL_MIN_SHARE dans une discipline et seraient
# sinon absentes de toutes les listes. Pas de double compte : à l'étape 6, un
# article de Nature classé en PE10 ne compte que pour PE10.
MULTIDISC_TOP_N = 5               # nb de revues retenues (par SJR décroissant) ; 0 = option désactivée
MULTIDISC_CATEGORY = "Multidisciplinary"   # catégorie SJR qui définit une revue multidisciplinaire
MULTIDISC_MIN_ARTICLES = 5000     # articles minimum (périmètre, toutes disciplines) : écarte les
                                  # revues jeunes ou confidentielles au SJR élevé mais instable
MULTIDISC_SJR_IDS = None          # liste imposée d'identifiants SJR (colonne Sourceid) ;
                                  # si renseignée, remplace la sélection automatique

# --- Pays -----------------------------------------------------------------------
# France métropolitaine + outre-mer (codes ISO utilisés par OpenAlex)
PAYS_FR = ("FR", "GP", "MQ", "GF", "RE", "YT", "PM", "BL", "MF", "WF", "PF", "NC")

# --- Indicateurs (étape 6) -------------------------------------------------------
PERIMETRES = ("toutes_revues_openalex", "revues_sjr")   # périmètres calculés aux étapes 4 et 6
ETAB_COUNTRIES = PAYS_FR           # établissements proposés : institutions de ces pays
                                   # (ROR de l'institution ou de l'une de ses tutelles)
ETAB_MIN_ARTICLES = 100            # articles minimum sur la période (premier périmètre) pour
                                   # qu'un établissement figure dans les résultats
# Comparaison internationale
PAYS_COMPARAISON = None            # liste de codes ISO à comparer à la France ; None = automatique :
PAYS_COMPARAISON_N = 25            # les N pays qui publient le plus (premier périmètre, toute la période)
ETAB_INTERNATIONAL_RORS = None     # établissements étrangers à inclure (liste de ROR) ; None = automatique :
ETAB_INTERNATIONAL_N = 300         # les N plus gros établissements étrangers (0 = aucun)
ETAB_INTERNATIONAL_TYPES = ("education", "facility", "government", "healthcare", "nonprofit")
                                   # types OpenAlex retenus pour la sélection automatique
# Corrections de la lignée OpenAlex (rattachements erronés) :
# (ROR de l'institution, ROR de la tutelle à couper). Toute institution qui a la
# première dans sa lignée perd la tutelle coupée ET les tutelles de celle-ci.
# Le fichier inst_lineage.parquet n'est pas modifié : la correction est
# appliquée au calcul des indicateurs. Vérifier l'effet avant de relancer :
#   lineage_check.preview_lineage_cuts()
# Exemple (rattachements erronés repérés dans OpenAlex, laissés tels quels pour l'instant) :
#   ("05f82e368", "03b4d6f26"),   # Université Paris Cité  -/-> Université Paris Lumières
#   ("03b4d6f26", "04wez5e68"),   # Université Paris Lumières -/-> Université Paris 8 (lien inversé)
#   ("03b4d6f26", "013bkhk48"),   # Université Paris Lumières -/-> Université Paris Nanterre (lien inversé)
LINEAGE_CUTS = []
# ROR retirés de la liste des établissements (pas des établissements de recherche) ;
# leurs articles restent comptés pour les autres établissements
ETAB_EXCLUDE = [
    "0172v8z89",                  # Gouvernement de la République française
]

# Copublications (étape 6b, copublications.py) : pour chaque pays et établissement,
# ses partenaires (pays et établissements) avec le nombre d'articles cosignés.
COPUB_PARTENAIRES_N = 50           # partenaires gardés par entité et par type de partenaire
                                   # (les plus fréquents sur toute la période) ; None = tous
COPUB_MAX_ENTITES_PAR_ARTICLE = None   # ex. 50 : ignore les articles à très nombreuses
                                       # affiliations (grandes collaborations) pour les paires
                                       # établissement x établissement ; None = tous

# --- Publication (étape 7, publish.py) -------------------------------------------
# Fichiers : <DATA_DIR>/publication/<classement>/<collection>.jsonl.gz (+ .parquet)
# Base : MongoDB de l'application tableaux (https://github.com/dataesr/tableaux).
MONGO_URI = os.getenv("MONGO_URI")                       # ne jamais écrire l'URI en dur ici
MONGO_DB = os.getenv("MONGO_DB_NAME", "tableaux-staging")
TABLEAUX_BOARD_ID = "suivi-recherche"   # identifiant du tableau dans la collection « boards »
PUBLIER_COPUBLICATIONS = False     # biblio_copublications (volumineuse) : publiée par publish() et
                                   # chargée par publish_mongo() seulement si True (ou, pour Mongo,
                                   # demandée explicitement : publish_mongo(collections=[...]))

# --- Référentiel des établissements (étape 0, referentiel.py) ----------------------
# Sert au périmètre du sélecteur et aux noms / sigles / catégories affichés ; ne change
# aucun calcul. Jointure avec OpenAlex par le ROR.
REFERENTIEL_SOURCE = "paysage"     # "paysage" ou "scanr"
PAYSAGE_DUMP_URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/paysage_dump.jsonl.gz"
SCANR_ORGANIZATIONS_URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/organizations-v2.jsonl.gz"
REFERENTIEL_DIR = f"{DATA_ROOT}/referentiel"   # commun à tous les snapshots OpenAlex
# Catégories (Paysage, libellé usualNameFr) des établissements proposés dans le sélecteur
REFERENTIEL_CATEGORIES = [
    "Grand établissement relevant d'un autre département ministériel", "Grand établissement",
    "Communauté d'universités et établissements", "Établissement public expérimental",
    "Université", "Organisme de recherche",
    "Institut national polytechnique", "Institut ou école extérieur aux universités",
    "École normale supérieure", "Établissement d'enseignement supérieur privé d'intérêt général",
    "École d'ingénieurs", "École de commerce et de management",
    "École nationale supérieure d'ingénieurs", "Institut d'étude politique",
    "Centre de lutte contre le cancer", "Institut hospitalo-universitaire", "Centre hospitalier",
    "Établissement privé d'enseignement universitaire",
    "Établissement d'enseignement supérieur privé rattachés à un EPSCP",
]
# Groupes de comparaison des établissements (champ entites.groupe_etablissement : matrice et
# classements de tableaux). La catégorie principale de Paysage est souvent un statut
# juridique (« Grand établissement » pour l'Université de Lorraine, « Grand établissement
# relevant d'un autre département ministériel » pour l'École polytechnique) : les règles
# lisent TOUTES les catégories Paysage de la structure (typologie « Université
# pluridisciplinaire avec santé », habilitation CTI...). Première règle satisfaite gagne,
# sinon « autres ». Une colonne « groupe » du fichier de corrections (DASHBOARD_ETABS_PATH)
# l'emporte. Codes et libellés alignés sur tableaux (suivi-recherche/matrice.js).
GROUPES_ETABLISSEMENTS = {
    "universites": "Universités",
    "organismes": "Organismes de recherche",
    "sante": "Établissements de santé",
    "ecoles-ingenieurs": "Écoles d'ingénieurs",
    "grands-etablissements": "Grands établissements, ENS et instituts",
    "commerce-iep": "Écoles de commerce et IEP",
    "prive": "Établissements privés",
    "autres": "Autres établissements",
}
GROUPES_ETABLISSEMENTS_REGLES = [
    ("organismes", ["Organisme de recherche"]),
    ("sante", ["Centre hospitalier", "Centre de lutte contre le cancer", "Institut hospitalo-universitaire"]),
    # typologie universitaire, avant les habilitations d'ingénieur (universités avec une école interne)
    ("universites", ["Université", "Université pluridisciplinaire avec santé",
                     "Université pluridisciplinaire hors santé", "Université scientifique et/ou médicale",
                     "Université tertiaire - droit et économie",
                     "Université tertiaire - lettres et sciences Humaines"]),
    ("ecoles-ingenieurs", ["École d'ingénieurs", "École nationale supérieure d'ingénieurs",
                           "Institut national polytechnique", "Université de technologie",
                           "École habilitée à délivrer un diplôme d'ingénieur",
                           "École ou site relevant d'une école accréditée à délivrer un titre d'ingénieur diplômé"]),
    ("commerce-iep", ["École de commerce et de management", "Institut d'étude politique"]),
    # regroupements sans typologie universitaire ni habilitation d'ingénieur
    ("universites", ["Établissement public expérimental", "Communauté d'universités et établissements"]),
    ("grands-etablissements", ["Grand établissement", "École normale supérieure",
                               "Grand établissement relevant d'un autre département ministériel",
                               "Institut ou école extérieur aux universités"]),
    ("prive", ["Établissement d'enseignement supérieur privé d'intérêt général",
               "Établissement privé d'enseignement universitaire",
               "Établissement d'enseignement supérieur privé rattachés à un EPSCP"]),
]

# --- Sélecteur d'établissements (entites.py) ------------------------------------------
# Établissements proposés = périmètre du référentiel (ci-dessus) qui ont des articles
# à l'étape 6, corrigé par un fichier CSV FACULTATIF (une ligne par établissement) :
#   ror ; nom (nom affiché, prioritaire) ; alias (termes de recherche séparés par |) ;
#   exclure (1 = retirer du sélecteur)
# Une ligne sans exclure AJOUTE l'établissement au sélecteur. Sans référentiel, le
# fichier seul définit le sélecteur ; sans les deux, tous les établissements français.
# Les autres établissements restent dans les données (comparaisons, copublications).
DASHBOARD_ETABS_PATH = "config/etablissements_dashboard.csv"
DASHBOARD_ORDRE_SELECTEUR = "taille"   # ordre quand la recherche est vide :
                                       # "taille" (plus gros d'abord) ou "fichier" (ordre du CSV)

# --- Open data ESR (Opendatasoft, utils_ods.py) ------------------------------------
# Jeux de données de data.enseignementsup-recherche.gouv.fr, téléchargés en CSV.
ODS_BASE_URL = "https://data.enseignementsup-recherche.gouv.fr"
ODS_API_KEY = os.getenv("ODS_API_KEY")   # facultative (jeux publics) ; jamais en dur ici

# --- Horizon Europe (horizon.py) --------------------------------------------------
# Participations aux projets Horizon Europe, deux jeux de même structure (une ligne par
# participation : projet x structure x rôle) : propositions évaluées et projets lauréats.
# Copie brute dans <DATA_DIR>/horizon/ ; sert aux ERC (erc.py) et à d'autres programmes.
HORIZON_ODS_SOUMIS = "fr-esr-horizon-projets-soumis-candidats"   # stage = evaluated
HORIZON_ODS_LAUREATS = "fr-esr-horizon-projects-entities"       # stage = successful
HORIZON_DIR = f"{DATA_DIR}/horizon"
HORIZON_COLONNES_EXCLUES = ("abstract",)   # textes longs non conservés dans la copie

# --- Bourses ERC (erc.py) ---------------------------------------------------------
# Candidatures et lauréats ERC par entité (établissement, pays, monde), année d'appel,
# panel (= discipline_code de la bibliométrie) et type de bourse (STG, COG, ADG, SYG, POC).
# Lignes ERC des jeux Horizon Europe (thema_code = ERC_THEMA), porteurs seulement
# (erc_role = ERC_ROLE_PI). Sorties dans <DATA_DIR>/erc/ : la lignée OpenAlex du snapshot
# sert au rattachement.
ERC_DIR = f"{DATA_DIR}/erc"
ERC_THEMA = "ERC"
ERC_ROLE_PI = "PI"
ERC_TYPES_BOURSE = None            # ex. ("STG", "COG", "ADG", "SYG") ; None = tous
ERC_RATTACHEMENT_LIGNEE = True     # une candidature compte aussi pour les tutelles de
                                   # l'établissement d'accueil (lignée OpenAlex, comme les articles)
ERC_PAYS_COMPARAISON = None        # codes ISO à comparer ; None = automatique :
ERC_PAYS_COMPARAISON_N = 25        # les N pays qui déposent le plus de candidatures
                                   # (tous types de bourse). L'étape 6 calcule aussi les
                                   # effectifs bibliométriques de ces pays : lancer compute_erc()
                                   # AVANT compute_indicators() (voir pipeline.py).
# Types de bourse de référence (calcul par défaut du tableau, pays comparés, années provisoires).
# Les autres types restent dans les données (SYG, POC...).
ERC_TYPES_PRINCIPAUX = ("STG", "COG", "ADG")
# Années d'appel provisoires (résultats pas encore tous publiés) :
#   liste imposée, ex. [2025] ; None = détection automatique : une année est provisoire si
#   - l'un des ERC_TYPES_PRINCIPAUX, présent les autres années, n'y a aucune candidature
#     ou aucun lauréat (projets), ou si
#   - ses candidatures (types principaux, MONDE) sont inférieures à ERC_SEUIL_ANNEE_COMPLETE
#     fois la médiane des autres années.
ERC_ANNEES_PROVISOIRES = None
ERC_SEUIL_ANNEE_COMPLETE = 0.75

# --- Prix et distinctions (prix.py) ---------------------------------------------------
# Lauréats des prix recensés dans Paysage (IUF, médailles du CNRS, prix des académies...),
# une ligne par lauréat d'un prix une année. Jeu non public : clé ODS_API_KEY.
# Sorties dans <DATA_DIR>/prix/ : la lignée OpenAlex du snapshot sert au rattachement.
PRIX_ODS = "fr_esr_paysage_laureats_all"
PRIX_DIR = f"{DATA_DIR}/prix"
PRIX_COLONNES_EXCLUES = ("prix_detail",)   # textes longs non conservés dans la copie
PRIX_RATTACHEMENT_LIGNEE = True    # un lauréat compte aussi pour les tutelles OpenAlex de
                                   # ses établissements (comme les articles et les ERC)
PRIX_ANNEE_MIN = None              # ex. 2000 : années antérieures écartées ; None = toutes
PRIX_PUBLIER_LAUREATS = True       # collection prix_laureats (liste nominative des lauréats)
# Groupes de prix proposés par le tableau : un prix appartient au groupe si son identifiant
# est dans "prix", ou l'un de ses porteurs dans "porteurs", ou l'une de ses catégories dans
# "categories" (identifiants et libellés Paysage). Le premier groupe est celui par défaut.
PRIX_GROUPES = [
    {"code": "IUF", "libelle": "Institut universitaire de France (membres juniors et seniors)",
     "porteurs": ["IPU6Y"]},
    {"code": "MEDAILLES_CNRS", "libelle": "Médailles du CNRS", "categories": ["Médaille du CNRS"]},
]
# Discipline (panel ERC) des lauréats, par leurs publications OpenAlex (prix_disciplines.py) :
# auteur retrouvé par ORCID, à défaut par nom + établissement ; topics -> panels par la table
# de l'étape 3. L'extraction relit les works du snapshot (long, comme l'étape 1).
PRIX_PUB_ANNEE_MIN = 1990          # publications lues (lauréats anciens : années 1990-2000)
PRIX_DISC_FENETRE = 10             # années de publications jusqu'au prix (année du prix comprise)
PRIX_DISC_MIN_PUBLICATIONS = 3     # moins de publications dans la fenêtre : toutes celles lues
PRIX_DISC_NOMS = True              # sans ORCID : appariement par nom + établissement du lauréat

# --- Viviers de chercheurs (viviers.py) ----------------------------------------------
# Chercheurs « actifs » par pays, par établissement français et par discipline, d'après leurs
# publications OpenAlex (auteurs OpenAlex qui signent dans l'un des pays lus). Sert de
# dénominateur aux ERC, aux projets (Horizon Europe, ANR...) et aux prix : candidats ou
# lauréats rapportés au vivier. L'extraction relit les works du snapshot (long, comme l'étape 1).
# Sorties dans <DATA_DIR>/viviers/.
VIVIERS_DIR = f"{DATA_DIR}/viviers"
# Pays lus : None = FR + pays de la comparaison ERC (erc_meta.pays_compares) + pays de la
# comparaison bibliométrique (biblio_meta.pays_compares, dont États-Unis, Chine...) : lancer
# compute_erc() et compute_indicators() avant l'extraction ; ou liste imposée de codes ISO
# (FR toujours ajouté). Changer la liste relit les works.
VIVIER_PAYS = None
VIVIER_PERIMETRE = PERIMETRE       # articles retenus = fichier top10 de ce périmètre (étape 4)
                                   # (+ livres et chapitres SHS, voir plus bas)
# Période d'activité du vivier, propre aux viviers (indépendante de ANNEE_MIN / ANNEE_MAX) :
# à changer explicitement quand on veut faire glisser le vivier (relit les works).
VIVIER_ANNEE_DEBUT = 2021
VIVIER_ANNEE_FIN = 2025
# Publications extraites depuis le début de la période. Abaisser pour tester d'autres
# périodes sans relire les works.
VIVIER_PUB_ANNEE_MIN = VIVIER_ANNEE_DEBUT
# Livres et chapitres : comptent comme des publications (activité, discipline, rattachements)
# quand leur panel est dans VIVIER_LIVRES_DOMAINES (SHS par défaut : hors SHS, OpenAlex classe
# beaucoup d'actes de conférence en book-chapter, ex. Springer LNCS). Jamais « top 10 % » (pas
# de référence de citations). None ou () : articles seuls.
VIVIER_LIVRES_TYPES = ("book", "book-chapter")
VIVIER_LIVRES_DOMAINES = ("SH",)
VIVIER_PUB_TYPES = ("article", "review") + tuple(VIVIER_LIVRES_TYPES or ())   # types extraits
# Chercheur actif : au moins VIVIER_MIN_PUBLICATIONS publications (articles du périmètre, plus
# livres et chapitres SHS) dans la période, dont au moins VIVIER_MIN_PUBLICATIONS_RECENTES
# dans les VIVIER_ANNEES_RECENTES dernières années.
VIVIER_MIN_PUBLICATIONS = 3
VIVIER_ANNEES_RECENTES = 2
VIVIER_MIN_PUBLICATIONS_RECENTES = 1
VIVIER_MAX_AUTEURS_PAR_ARTICLE = 100   # publications à plus d'auteurs (grandes collaborations) ignorées
                                       # pour tous les calculs du vivier ; None = tous
# Rattachement : un chercheur compte pour un établissement si au moins cette part de ses
# articles récents (VIVIER_ANNEES_RECENTES, signatures des pays lus) y est affiliée
# (lignée comprise : UMR -> université et CNRS). 0.5 = établissement principal.
VIVIER_PART_MIN_ETABLISSEMENT = 0.5
VIVIER_PART_MIN_PAYS = 0.5          # idem pour le pays (même règle pour tous les pays, France comprise)
# Top 10 % : un article compte comme « dans le top 10 % » si sa fraction_top10_cites (étape 4)
# atteint ce seuil (1 = nettement au-dessus du seuil de citations, < 1 = ex aequo au seuil).
VIVIER_SEUIL_FRACTION_TOP10 = 0.5
VIVIER_MIN_TOP10 = 1               # articles du top 10 % pour être « chercheur top 10 % »
VIVIER_MIN_AUTEUR_PRINCIPAL = 1    # articles en premier ou dernier auteur, ou correspondant
VIVIER_PUBLIER_CHERCHEURS = True   # collection viviers_chercheurs (liste nominative)

# --- Financements OpenAlex (awards, extract_awards.py) ----------------------------------
# Awards (financements déclarés : ANR, NIH, DFG...) du snapshot, par pays du FINANCEUR
# (country_code de l'entité funder ; outre-mer regroupé sous FR comme partout, PAYS_FR) et par
# panel ERC (topic principal de l'award -> discipline, table de l'étape 3).
# Snapshot des awards indépendant de celui des works (SNAPSHOT) : les financements sont lus
# dans le snapshot le plus récent, de source SNAPSHOT_SOURCE ("daily" = offre payante, clé
# OPENALEX_API_KEY). Le classement topic -> panel reste celui de DATA_DIR (étape 3).
# Sorties dans <AWARDS_ROOT>/<date du snapshot des awards>/ (les anciens sont conservés).
AWARDS_ROOT = f"{DATA_ROOT}/awards"
AWARDS_SNAPSHOT = None             # None = dernier snapshot quotidien à l'extraction (puis, pour
                                   # compute_awards, le plus récent extrait) ; "AAAA-MM-JJ" pour figer
AWARDS_ANNEE_MIN = None            # ex. 2015 : awards dont start_year est antérieure écartés à
                                   # l'extraction ; None = tous (y compris sans start_year)
# Financeurs internationaux rattachés à un pays par OpenAlex (siège) : comptés sous le pays
# AWARDS_PAYS_INTERNATIONAL au lieu de leur pays. [] = pays d'OpenAlex pour tous.
AWARDS_PAYS_INTERNATIONAL = "INT"
AWARDS_FINANCEURS_INTERNATIONAUX = [
    "F4320320338", "F4320307846",  # Human Frontier Science Program (deux fiches)
    "F4320320887",                 # European Synchrotron Radiation Facility
    "F4320318240",                 # European Space Agency
    "F4320320301",                 # European Science Foundation
    "F4320338463",                 # CHIST-ERA
    "F4320320885",                 # European Research Consortium for Informatics and Mathematics
    "F4320308321",                 # UNESCO
    "F4320310728",                 # Centre international de recherche sur le cancer
]

# --- DuckDB ---------------------------------------------------------------------
DUCKDB_MEMORY_LIMIT = None    # ex. "16GB" ; None = défaut DuckDB (80 % de la RAM)
DUCKDB_THREADS = None         # None = tous les cœurs
