from project.server.main.copublications import compute_copublications
from project.server.main.disciplines import build_disciplines
from project.server.main.export_dashboard import build_dashboard
from project.server.main.extract_works import extract_snapshot, summary, check_duplicates
from project.server.main.indicators import compute_indicators
from project.server.main.logger import get_logger
from project.server.main.openalex_s3 import diagnose_daily
from project.server.main.projects import generate_projects
from project.server.main.publish import publish, publish_mongo
from project.server.main.referentiel import build_referentiel
from project.server.main.top_journals import compute_top_journals, top_journals_from_review
from project.server.main.top10_citations import compute_top10

logger = get_logger(__name__)


def create_task_compute(args: dict) -> None:
    # Tous les chemins se règlent dans config.py : DATA_ROOT = /data, SNAPSHOT = "2026-10-15"
    # -> DATA_DIR = /data/snapshots/2026-10-15. Les noms de variables des fichiers
    # produits sont décrits dans CONTRAT_DONNEES.md.

# ---------------------------------------------------------------------------
# 0. Référentiel des établissements français (moins d'une minute)
#    Télécharge : paysage_dump.jsonl.gz (scanR, REFERENTIEL_SOURCE = "paysage")
#                 ou organizations-v2.jsonl.gz (REFERENTIEL_SOURCE = "scanr")
#    Écrit : /data/referentiel/paysage.parquet (une ligne par structure avec un ROR :
#            nom d'usage, sigle, catégorie, ville, dans_perimetre)
#    Ne change aucun calcul : sert au périmètre du sélecteur et aux noms affichés.
#    Périmètre : structures de tête, actives, avec ROR, des catégories de
#    REFERENTIEL_CATEGORIES (config.py). Remplace l'ancien paysage.get_perimeter().
# ---------------------------------------------------------------------------
    build_referentiel()

# ---------------------------------------------------------------------------
# 1. Vérifier l'accès aux snapshots quotidiens (facultatif, quelques secondes)
#    Télécharge : des identifiants S3 temporaires (api.openalex.org) et la liste
#                 des dossiers du bucket. Écrit : rien.
#    Attendu : une ligne "s3_list OK" suivie de dates.
# ---------------------------------------------------------------------------
    #diagnose_daily()

# ---------------------------------------------------------------------------
# 2. Extraire le snapshot du 15 octobre : étapes 1 et 2 (le plus long : 1 à 2 h)
#    Lit sur S3 (s3://openalex-snapshots/full/2026-10-15/parquet/) :
#      - works : seulement les colonnes utiles (~27 Go lus, rien n'est stocké en entier),
#        articles et reviews publiés depuis 2015 ;
#      - sources et institutions : tables entières, petites.
#    Écrit dans /data/snapshots/2026-10-15/ :
#      works_manifest.json     liste des fichiers du snapshot (sert à reprendre si coupure)
#      works/*.parquet         une ligne par article : année, type, topic, revue,
#                              citations, pays des auteurs
#      work_inst/*.parquet     une ligne par (article, institution)
#      sources.parquet         revues OpenAlex et leurs ISSN
#      institutions.parquet    institutions, ROR, pays, type, tutelles, sigles
#      inst_lineage.parquet    (institution, tutelle) dépliée pour le filtre établissement
#    En cas d'interruption : relancer la même commande, seul ce qui manque est relu.
#    Déjà fait pour le 15 octobre : inutile de relancer (les fichiers sont réutilisés).
# ---------------------------------------------------------------------------
    #res = extract_snapshot("2026-10-15")

    # Contrôles (lecture locale seulement, rien n'est téléchargé ni écrit)
    #summary()             # volumes par année, part avec un pays, nombre d'articles France
    #check_duplicates()    # "doublons" doit valoir 0

# ---------------------------------------------------------------------------
# 3. Classer les topics en disciplines (quelques minutes)
#    Lit : topics/openalex_topics_erc_panels.json (dans /src) et works/
#    Écrit : disciplines/erc.parquet (topic_id -> discipline_code, discipline_nom, domaine_code)
#    Attendu : "Articles classés : … (100.0 %)"
# ---------------------------------------------------------------------------
    build_disciplines()

# ---------------------------------------------------------------------------
# 4. Revues de tête (quelques minutes)
#    Lit : scimago/scimagojr_2025.csv (dans /src), sources.parquet, works/, disciplines/
#    Écrit dans revues_tete/ :
#      sources_sjr.parquet             correspondance revues OpenAlex <-> SJR (par ISSN)
#      erc.parquet                     revues de tête par discipline (+ multidisciplinaires)
#      erc_relecture.csv               candidates par discipline, à relire dans un tableur
#      erc_multidisciplinaires.csv     candidates multidisciplinaires, à relire
#      erc_params.json                 paramètres utilisés
#    À passer AVANT l'étape 5 : le périmètre "revues_sjr" a besoin de sources_sjr.parquet.
# ---------------------------------------------------------------------------
    compute_top_journals()

# 4 bis (facultatif). Réappliquer votre liste corrigée à la main.
#    Enregistrez le CSV relu sous un autre nom dans le dossier du snapshot, puis :
#    Lit : le CSV relu (colonne "retenue"). Écrit : revues_tete/erc.parquet (remplacé)
# top_journals_from_review("/data/snapshots/2026-10-15/revues_tete/erc_relecture_validee.csv",
#                          out_name="erc")

# ---------------------------------------------------------------------------
# 5. Top 10 % des articles les plus cités, une fois par périmètre (10 à 30 min chacun)
#    Lit : works/, disciplines/, revues_tete/sources_sjr.parquet (périmètre "revues_sjr")
#    Écrit dans top10/ :
#      erc_toutes_revues_openalex.parquet / erc_revues_sjr.parquet
#                                      fraction_top10_cites de chaque article
#      *_seuils.parquet                seuil de citations et ex aequo par discipline x année
#      *_params.json                   paramètres (fenêtre de citation, années couvertes)
#    Utilise /data/snapshots/2026-10-15/tmp_duckdb/ comme espace temporaire.
# ---------------------------------------------------------------------------
    compute_top10(perimetre="toutes_revues_openalex")
    compute_top10(perimetre="revues_sjr")

# ---------------------------------------------------------------------------
# 6. Effectifs : monde, 25 pays, établissements français et 300 étrangers (15 à 45 min)
#    Lit : top10/, works/ (pays, revue), work_inst/, institutions.parquet,
#          inst_lineage.parquet, revues_tete/
#    Écrit dans indicateurs/ :
#      erc_effectifs.parquet       nombres d'articles par périmètre x entité x année x discipline
#      erc_entites.parquet         entités OpenAlex (ROR, nom, pays, type, taille, tutelles)
#      erc_params.json             paramètres (pays comparés, seuils, corrections)
#    Affiche : la France par année, les plus gros établissements, les pays comparés.
# ---------------------------------------------------------------------------
    compute_indicators()

# ---------------------------------------------------------------------------
# 6 bis. Copublications (10 à 30 min ; plus si beaucoup de grandes collaborations,
#    voir COPUB_MAX_ENTITES_PAR_ARTICLE dans config.py)
#    Lit : les mêmes fichiers que l'étape 6, plus indicateurs/erc_entites.parquet
#    Écrit : indicateurs/erc_copublications.parquet (entité x partenaire x année x discipline)
# ---------------------------------------------------------------------------
    compute_copublications()

# ---------------------------------------------------------------------------
# 7. Publication pour l'application tableaux (une à deux minutes)
#    Lit : indicateurs/, revues_tete/, top10/*_seuils, disciplines/, le référentiel,
#          config/etablissements_dashboard.csv (corrections, facultatif)
#    Écrit dans publication/erc/ : entites, biblio_disciplines, biblio_effectifs,
#          biblio_copublications, biblio_revues_tete, biblio_seuils_top10, biblio_meta
#          (.jsonl.gz pour Mongo, .parquet), et manifest.json
# ---------------------------------------------------------------------------
    publish()

# 7 bis. Chargement dans MongoDB (base de tableaux) : MONGO_URI et MONGO_DB_NAME
#    en variables d'environnement. Crée une collection <nom>_<horodatage> par
#    collection et enregistre la version dans « boards ».
    #publish_mongo()

# ---------------------------------------------------------------------------
# 8. Aperçu HTML, pour vérifier sans l'application tableaux (moins d'une minute)
#    Écrit : apercu/index.html (page complète, données incluses) et bibliometrie.json.gz
# ---------------------------------------------------------------------------
    build_dashboard()

# ---------------------------------------------------------------------------
# 9. Facultatif : diagnostic des rattachements d'établissements (lecture seule)
#    Lit : institutions.parquet, inst_lineage.parquet, work_inst/. Écrit : rien.
# ---------------------------------------------------------------------------
#from project.server.main.lineage_check import suspicious_ancestors
#suspicious_ancestors()

def create_task_projects() -> None:
    generate_projects()