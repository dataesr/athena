import time

from project.server.main.copublications import compute_copublications
from project.server.main.disciplines import build_disciplines
from project.server.main.erc import compute_erc
from project.server.main.prix import compute_prix
from project.server.main.prix_disciplines import compute_prix_disciplines
from project.server.main.publications_auteurs import extraire_publications
from project.server.main.export_dashboard import build_dashboard
from project.server.main.extract_works import extract_snapshot, summary, check_duplicates
from project.server.main.pipeline import step_awards
from project.server.main.indicators import compute_indicators
from project.server.main.startups import compute_startups
from project.server.main.logger import get_logger
from project.server.main.openalex_s3 import diagnose_daily
from project.server.main.projects import generate_projects
from project.server.main.publish import publish, publish_mongo
from project.server.main.config import PERIMETRES, SNAPSHOT
from project.server.main.referentiel import build_referentiel, build_tutelles_rnsr
from project.server.main.top_journals import compute_top_journals, top_journals_from_review
from project.server.main.top10_citations import compute_top10
from project.server.main.viviers import compute_viviers

logger = get_logger(__name__)


# Étapes de create_task_compute, dans l'ordre d'exécution. Choix par le JSON de POST /compute :
#   {}                                          étapes PAR_DEFAUT (toute la chaîne, comme avant)
#   {"steps": ["viviers", "publish"]}           ces étapes seulement (toujours dans l'ordre ci-dessous)
#   {"start": "publications", "end": "publish"} plage des étapes PAR_DEFAUT (bornes incluses)
#   {"steps": [...], "params": {"viviers": {"annee_debut": 2016, "out_suffix": "_2016"}}}
#                                               arguments passés à la fonction d'une étape
#   GET /compute                                liste des étapes (sans rien lancer)
# Résultat de la tâche (GET /tasks/<id>) : durée en minutes de chaque étape lancée.
ETAPES = ["diagnose_daily", "referentiel", "tutelles_rnsr", "extract_snapshot", "summary",
          "check_duplicates", "disciplines", "top_journals", "top10", "erc", "prix", "indicators",
          "publications", "prix_disciplines", "viviers", "copublications", "awards",
          "startups", "publish", "publish_mongo", "dashboard"]
# Lancées seulement si demandées explicitement dans "steps" (contrôles, chargement en base)
HORS_DEFAUT = ["diagnose_daily", "summary", "check_duplicates", "awards", "startups", "publish_mongo"]
PAR_DEFAUT = [e for e in ETAPES if e not in HORS_DEFAUT]


def etapes_choisies(args: dict) -> list:
    """Étapes à lancer d'après le JSON de /compute (voir ETAPES)."""
    args = args or {}
    steps, start, end = args.get("steps"), args.get("start"), args.get("end")
    if isinstance(steps, str):
        steps = [steps]
    for e in (steps or []) + [x for x in (start, end) if x] + list(args.get("params") or {}):
        if e not in ETAPES:
            raise ValueError(f"étape inconnue : {e!r} ; disponibles : {ETAPES}")
    if steps:
        return [e for e in ETAPES if e in steps]
    i0 = PAR_DEFAUT.index(start) if start in PAR_DEFAUT else 0
    i1 = PAR_DEFAUT.index(end) if end in PAR_DEFAUT else len(PAR_DEFAUT) - 1
    if (start and start not in PAR_DEFAUT) or (end and end not in PAR_DEFAUT):
        raise ValueError(f"start / end parmi {PAR_DEFAUT} ; {HORS_DEFAUT} : par \"steps\"")
    return PAR_DEFAUT[i0:i1 + 1]


def awards(**kw) -> dict:
    """Étape awards (pipeline.step_awards) ; résultat réduit à des valeurs simples (résultat de tâche)."""
    r = step_awards(**kw)
    cov = r["couverture"].set_index("pays_code")
    return {"snapshot_awards": r["snapshot_awards"], "fichiers": r["fichiers"], "topics_non_classes": len(r["non_classes"]),
            "couverture_fr": cov.loc["FR"].to_dict() if "FR" in cov.index else None}


def create_task_compute(args: dict) -> dict:
    # Tous les chemins se règlent dans config.py : DATA_ROOT = /data, SNAPSHOT = "2026-10-15"
    # -> DATA_DIR = /data/snapshots/2026-10-15. Les noms de variables des fichiers
    # produits sont décrits dans CONTRAT_DONNEES.md.
    args = args or {}
    choix = etapes_choisies(args)
    params = args.get("params") or {}
    res = {}
    logger.info(f"Étapes lancées : {choix}")

    def etape(nom, fn, *a, **kw):
        if nom not in choix:
            return None
        kw = {**kw, **params.get(nom, {})}
        logger.info(f"===== {nom} : {fn.__name__}({', '.join(f'{k}={v!r}' for k, v in kw.items())})")
        t0 = time.time()
        r = fn(*a, **kw)
        res[nom] = round(res.get(nom, 0) + (time.time() - t0) / 60, 1)
        if nom in HORS_DEFAUT and isinstance(r, dict):   # contrôles : résultat renvoyé tel quel
            res[f"{nom}_resultat"] = r
        if isinstance(r, dict) and r.get("erreurs"):    # extraction : fichiers works non lus
            raise RuntimeError(f"{nom} : {len(r['erreurs'])} fichier(s) non lus, relancer (reprise possible)")
        return r

# ---------------------------------------------------------------------------
# 0. Référentiel des établissements français (moins d'une minute)
#    Télécharge : paysage_dump.jsonl.gz (scanR, REFERENTIEL_SOURCE = "paysage")
#                 ou organizations-v2.jsonl.gz (REFERENTIEL_SOURCE = "scanr")
#    Écrit aussi paysage_ror.parquet et paysage_successeurs.parquet (structures
#            fusionnées -> structure actuelle), utilisés par l'étape ERC
#    Écrit : /data/referentiel/paysage.parquet (une ligne par structure avec un ROR :
#            nom d'usage, sigle, catégorie, ville, dans_perimetre)
#    Ne change aucun calcul : sert au périmètre du sélecteur et aux noms affichés.
#    Périmètre : structures de tête, actives, avec ROR, des catégories de
#    REFERENTIEL_CATEGORIES (config.py). Remplace l'ancien paysage.get_perimeter().
# ---------------------------------------------------------------------------
    etape("referentiel", build_referentiel)
    etape("tutelles_rnsr", build_tutelles_rnsr)      # tutelles des laboratoires RNSR (scanR), pour les ERC et les prix

# ---------------------------------------------------------------------------
# 1. Vérifier l'accès aux snapshots quotidiens (facultatif, quelques secondes)
#    Télécharge : des identifiants S3 temporaires (api.openalex.org) et la liste
#                 des dossiers du bucket. Écrit : rien.
#    Attendu : une ligne "s3_list OK" suivie de dates.
# ---------------------------------------------------------------------------
    etape("diagnose_daily", diagnose_daily)

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
#    Date : SNAPSHOT dans config.py. Si le snapshot est déjà extrait, les fichiers sont
#    réutilisés (seul ce qui manque est relu).
# ---------------------------------------------------------------------------
    etape("extract_snapshot", extract_snapshot, SNAPSHOT)

    # Contrôles (lecture locale seulement, rien n'est téléchargé ni écrit)
    etape("summary", summary)             # volumes par année, part avec un pays, nombre d'articles France
    etape("check_duplicates", check_duplicates)    # "doublons" doit valoir 0

# ---------------------------------------------------------------------------
# 3. Classer les topics en disciplines (quelques minutes)
#    Lit : topics/openalex_topics_erc_panels.json (dans /src) et works/
#    Écrit : disciplines/erc.parquet (topic_id -> discipline_code, discipline_nom, domaine_code)
#    Attendu : "Articles classés : … (100.0 %)"
# ---------------------------------------------------------------------------
    etape("disciplines", build_disciplines)

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
    etape("top_journals", compute_top_journals)

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
    # params {"top10": {"perimetre": "revues_sjr"}} : ce seul périmètre
    for perimetre in ([None] if "perimetre" in params.get("top10", {}) else PERIMETRES):
        etape("top10", compute_top10, **({"perimetre": perimetre} if perimetre else {}))

# ---------------------------------------------------------------------------
# 5 bis. Bourses ERC (quelques minutes) : AVANT l'étape 6, qui calcule aussi
#    les articles des pays de la comparaison ERC
#    Télécharge : les participations Horizon Europe de l'open data ESR, propositions
#                 évaluées (fr-esr-horizon-projets-soumis-candidats, non public :
#                 ODS_API_KEY en variable d'environnement) et projets lauréats
#                 (fr-esr-horizon-projects-entities) ; copies dans horizon/
#                 (compute_erc(telecharger=False) les relit)
#    Lit : /data/referentiel/ (construits au besoin) : paysage_ror.parquet et
#          paysage_successeurs.parquet (dump Paysage), rnsr_tutelles.parquet (organisations
#          scanR : tutelles des laboratoires RNSR) ; inst_lineage.parquet et
#          institutions.parquet (rattachement aux tutelles OpenAlex)
#    Écrit dans erc/ : erc_effectifs.parquet, erc_entites.parquet, erc_meta.json,
#          erc_non_rattaches.csv
#    Affiche : rattachement à un ROR, mobilité des lauréats, la France par année.
# ---------------------------------------------------------------------------
    etape("erc", compute_erc)

# ---------------------------------------------------------------------------
# 5 ter. Prix et distinctions (une à deux minutes)
#    Télécharge : les lauréats des prix recensés dans Paysage (fr_esr_paysage_laureats_all,
#                 non public : ODS_API_KEY) ; copie dans prix/prix_brut.parquet
#                 (compute_prix(telecharger=False) la relit)
#    Lit : /data/referentiel/ (comme l'étape ERC), inst_lineage.parquet et institutions.parquet
#    Écrit dans prix/ : prix_effectifs.parquet, prix_catalogue.parquet, prix_laureats.parquet,
#          prix_entites.parquet, prix_meta.json, prix_non_rattaches.csv
#    Affiche : rattachement aux établissements, effectifs des groupes de prix (PRIX_GROUPES).
# ---------------------------------------------------------------------------
    etape("prix", compute_prix)

# ---------------------------------------------------------------------------
# 6. Effectifs : monde, 25 pays (+ pays de la comparaison ERC), établissements
#    français et 300 étrangers (15 à 45 min)
#    Lit : top10/, works/ (pays, revue), work_inst/, institutions.parquet,
#          inst_lineage.parquet, revues_tete/
#    Écrit dans indicateurs/ :
#      erc_effectifs.parquet       nombres d'articles par périmètre x entité x année x discipline
#      erc_entites.parquet         entités OpenAlex (ROR, nom, pays, type, taille, tutelles)
#      erc_params.json             paramètres (pays comparés, seuils, corrections)
#    Affiche : la France par année, les plus gros établissements, les pays comparés.
# ---------------------------------------------------------------------------
    etape("indicators", compute_indicators)

# ---------------------------------------------------------------------------
# 6 ter. Publications des lauréats de prix et signatures des viviers : UNE lecture des works
#    Lit sur S3 : les works du snapshot (de l'ordre de l'étape 2, reprise possible, fichier
#                 par fichier ; chaque sortie a sa reprise : de nouveaux lauréats ne relisent
#                 que pour les prix, un changement de pays ou de période que pour les viviers)
#    Après compute_prix() (lauréats cherchés), compute_erc() et compute_indicators() (pays).
#    Écrit : prix/publications/*.parquet, viviers/auteurs/*.parquet
# ---------------------------------------------------------------------------
    etape("publications", extraire_publications)

# ---------------------------------------------------------------------------
# 6 quater. Discipline (panel ERC) des lauréats de prix, par leurs publications
#    (règle commune avec les viviers : publications_auteurs.poids_disciplines)
#    Lit : prix/prix_personnes.parquet, prix/publications/, disciplines/, inst_lineage.parquet
#    Écrit dans prix/ : prix_laureats_disciplines.parquet,
#          prix_effectifs_disciplines.parquet, prix_disciplines_meta.json
# ---------------------------------------------------------------------------
    etape("prix_disciplines", compute_prix_disciplines)

# ---------------------------------------------------------------------------
# 6 quinquies. Viviers de chercheurs : chercheurs actifs par pays (France + pays des comparaisons
#    ERC et bibliométrique, après compute_erc() et compute_indicators()), par établissement
#    français et par discipline
#    Lit : viviers/auteurs/ (étape 6 ter), top10/, disciplines/, institutions.parquet, inst_lineage.parquet,
#          indicateurs/erc_entites.parquet
#    Écrit dans viviers/ : viviers_chercheurs.parquet,
#          viviers_effectifs.parquet, viviers_meta.json
#    Période : VIVIER_ANNEE_DEBUT-VIVIER_ANNEE_FIN (2021-2025), propre aux viviers ; les
#    publications ne sont extraites que depuis son début : l'avancer relit les works.
#    Seuils (chercheur actif, top 10 %) : VIVIER_* dans config.py ; les changer ne demande
#    que compute_viviers().
# ---------------------------------------------------------------------------
    etape("viviers", compute_viviers)

# ---------------------------------------------------------------------------
# 6 bis. Copublications (10 à 30 min ; plus si beaucoup de grandes collaborations,
#    voir COPUB_MAX_ENTITES_PAR_ARTICLE dans config.py)
#    Lit : les mêmes fichiers que l'étape 6, plus indicateurs/erc_entites.parquet
#    Écrit : indicateurs/erc_copublications.parquet (entité x partenaire x année x discipline)
# ---------------------------------------------------------------------------
    etape("copublications", compute_copublications)

# ---------------------------------------------------------------------------
# 6c. Financements OpenAlex (awards) : seulement si demandé ({"steps": ["awards"]})
#    Lit sur S3 : awards (sous-colonnes utiles) et funders du snapshot quotidien le plus récent
#    (AWARDS_SNAPSHOT pour figer une date ; indépendant de SNAPSHOT, celui des works).
#    Écrit dans /data/awards/<date>/ : extraits/*.parquet (une ligne par award), funders.parquet,
#          erc_awards.parquet (award -> pays du financeur, panel), erc_pays_disciplines.parquet
#    Pays du financeur : outre-mer regroupé sous FR (PAYS_FR) ; financeurs internationaux
#    (AWARDS_FINANCEURS_INTERNATIONAUX) sous "INT". Reprise : seuls les fichiers manquants sont lus.
# ---------------------------------------------------------------------------
    etape("awards", awards)

# ---------------------------------------------------------------------------
# 6d. Pilote Innovation : créations annuelles de startups ESR (facultatif)
#     Télécharge le jeu public fr-esr-startup-esr-france et résout les SIRET
#     liés vers scanR organizations-v2. Les données individuelles restent locales.
#     Écrit : innovation/startups_effectifs.parquet, startups_entites.parquet,
#             startups_meta.json et startups_non_rattaches.parquet
# ---------------------------------------------------------------------------
    etape("startups", compute_startups)

# ---------------------------------------------------------------------------
# 7. Publication pour l'application tableaux (une à deux minutes)
#    Lit : indicateurs/, revues_tete/, top10/*_seuils, disciplines/, le référentiel,
#          config/etablissements_dashboard.csv (corrections, facultatif)
#    Écrit dans publication/erc/ : entites, biblio_disciplines, biblio_effectifs,
#          biblio_copublications (si PUBLIER_COPUBLICATIONS), biblio_revues_tete, biblio_seuils_top10, biblio_meta,
#          erc_effectifs, erc_meta, prix_effectifs, prix_catalogue, prix_laureats, prix_meta,
#          prix_effectifs_disciplines, viviers_effectifs, viviers_chercheurs, viviers_meta
#          (.jsonl.gz pour Mongo, .parquet), et manifest.json
# ---------------------------------------------------------------------------
    etape("publish", publish)

# 7 bis. Chargement dans MongoDB (base de tableaux) : MONGO_URI et MONGO_DB_NAME
#    en variables d'environnement. Crée une collection <nom>_<horodatage> par
#    collection et enregistre la version dans « boards ».
    etape("publish_mongo", publish_mongo)

# ---------------------------------------------------------------------------
# 8. Aperçu HTML, pour vérifier sans l'application tableaux (moins d'une minute)
#    Écrit : apercu/index.html (page complète, données incluses) et bibliometrie.json.gz
# ---------------------------------------------------------------------------
    etape("dashboard", build_dashboard)

    logger.info(f"Terminé : {res}")
    return res

# ---------------------------------------------------------------------------
# 9. Facultatif : diagnostic des rattachements d'établissements (lecture seule)
#    Lit : institutions.parquet, inst_lineage.parquet, work_inst/. Écrit : rien.
# ---------------------------------------------------------------------------
#from project.server.main.lineage_check import suspicious_ancestors
#suspicious_ancestors()

def create_task_projects() -> None:
    generate_projects()