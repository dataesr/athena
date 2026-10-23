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
SNAPSHOT = "2026-10-15"            # figé sur le snapshot quotidien du 15 octobre 2026

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
TABLEAUX_BOARD_ID = "bibliometrie"   # identifiant du tableau dans la collection « boards »

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

# --- DuckDB ---------------------------------------------------------------------
DUCKDB_MEMORY_LIMIT = None    # ex. "16GB" ; None = défaut DuckDB (80 % de la RAM)
DUCKDB_THREADS = None         # None = tous les cœurs
