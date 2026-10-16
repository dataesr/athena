
from project.server.main.openalex_s3 import diagnose_daily
from project.server.main.extract_works import extract_snapshot, summary, check_duplicates
from project.server.main.logger import get_logger
from project.server.main.disciplines import build_disciplines
from project.server.main.top_journals import compute_top_journals, top_journals_from_review
from project.server.main.top10_citations import compute_top10
from project.server.main.indicators import compute_indicators
from project.server.main.export_dashboard import build_dashboard
from project.server.main.paysage import get_perimeter
logger = get_logger(__name__)


def create_task_compute(args: dict) -> None:
    # 0. Extrait le périmètre FR de paysage avec les ROR - n'impacte que le sélecteur dans le dashboard! Le reste vient d'OpenAlex
    get_perimeter()

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
#      extract_params.json     paramètres d'extraction (années, types)
#      works/*.parquet         une ligne par article : année, type, topic, revue,
#                              citations, pays des auteurs
#      work_inst/*.parquet     une ligne par (article, institution)
#      sources.parquet         revues OpenAlex et leurs ISSN
#      institutions.parquet    institutions, ROR, pays, type, tutelles
#      inst_lineage.parquet    (institution, tutelle) dépliée pour le filtre établissement
#    En cas d'interruption : relancer la même commande, seul ce qui manque est relu.
# ---------------------------------------------------------------------------
    res = extract_snapshot("2026-10-15")

    # Contrôles (lecture locale seulement, rien n'est téléchargé ni écrit)
    summary()             # volumes par année, part avec un pays, nombre d'articles France
    check_duplicates()    # "doublons" doit valoir 0

# ---------------------------------------------------------------------------
# 3. Classer les topics en disciplines (quelques minutes)
#    Lit : topics/openalex_topics_erc_panels.json (dans /src) et works/
#    Écrit : disciplines/erc.parquet (topic -> panel ERC)
#    Attendu : "Articles classés : … (100.0 %)"
# ---------------------------------------------------------------------------
    #build_disciplines()

# ---------------------------------------------------------------------------
# 4. Revues de tête (quelques minutes)
#    Lit : scimago/scimagojr_2025.csv (dans /src), sources.parquet, works/, disciplines/
#    Écrit dans top_journals/ :
#      sjr_sources.parquet             correspondance revues OpenAlex <-> SJR (par ISSN)
#      erc.parquet                     revues de tête par discipline (+ multidisciplinaires)
#      erc_revue.csv                   candidates par discipline, à relire dans un tableur
#      erc_multidisciplinaires.csv     candidates multidisciplinaires, à relire
#      erc_params.json                 paramètres utilisés
#    À passer AVANT l'étape 5 : le monde "sjr" a besoin de sjr_sources.parquet.
# ---------------------------------------------------------------------------
    #compute_top_journals()

# 4 bis (facultatif). Réappliquer votre liste corrigée à la main.
#    Recopiez d'abord le CSV sauvegardé dans le dossier du snapshot, puis :
#    Lit : le CSV relu (colonne "retenue"). Écrit : top_journals/erc.parquet (remplacé)
# top_journals_from_review("/data/snapshots/2026-10-15/top_journals/erc_revue_valide.csv",
#                          out_name="erc")

# ---------------------------------------------------------------------------
# 5. Top 10 % des articles les plus cités, une fois par monde (10 à 30 min chacun)
#    Lit : works/, disciplines/, top_journals/sjr_sources.parquet (monde "sjr")
#    Écrit dans top10/ :
#      erc.parquet / erc_sjr.parquet   poids de chaque article dans le top 10 % (w_top)
#      *_seuils.parquet                seuil de citations par discipline x année
#      *_params.json                   paramètres (fenêtre de citation, années couvertes)
#    Utilise /data/snapshots/2026-10-15/tmp_duckdb/ comme espace temporaire.
# ---------------------------------------------------------------------------
    #compute_top10()                # monde "all" : toutes les revues OpenAlex
    #compute_top10(world="sjr")     # monde "sjr" : revues indexées dans SJR

# ---------------------------------------------------------------------------
# 6. Indicateurs : France, 25 pays, établissements français et 300 étrangers (15 à 45 min)
#    Lit : top10/, works/ (pays, revue), work_inst/, institutions.parquet,
#          inst_lineage.parquet, top_journals/
#    Écrit dans indicators/ :
#      erc.parquet                 effectifs et ratios par monde x entité x année x discipline
#      erc_etablissements.parquet  liste des établissements (ROR, nom, pays, type, taille)
#      erc_params.json             paramètres (pays comparés, seuils, corrections)
#    Affiche : la France par année, les plus gros établissements, les pays comparés.
# ---------------------------------------------------------------------------
    #compute_indicators()

# ---------------------------------------------------------------------------
# 7. Générer le dashboard (moins d'une minute)
#    Lit : indicators/ et les fichiers *_params.json (pour la note méthodologique)
#    Écrit dans dashboard/ :
#      index.html             la page complète, données incluses (~3 Mo)
#      bibliometrie.json.gz   les mêmes données à part (pour une version future)
#    Ne télécharge rien. C'est la page, une fois ouverte dans un navigateur, qui charge
#    le DSFR et Chart.js depuis le CDN jsDelivr.
# ---------------------------------------------------------------------------
    build_dashboard()

# ---------------------------------------------------------------------------
# 8. Facultatif : diagnostic des rattachements d'établissements (lecture seule)
#    Lit : institutions.parquet, inst_lineage.parquet, work_inst/. Écrit : rien.
# ---------------------------------------------------------------------------
#from project.server.main.lineage_check import suspicious_ancestors
#suspicious_ancestors()
