# athena

Chaîne de calcul des indicateurs bibliométriques (poids, influence, prestige,
copublications) à partir des snapshots [OpenAlex](https://openalex.org), pour
l'onglet « bibliométrie » de l'application [tableaux](https://github.com/dataesr/tableaux).

Les noms de champs et les règles de calcul des données produites sont décrits
dans [CONTRAT_DONNEES.md](CONTRAT_DONNEES.md).

## Fonctionnement

- Une API Flask met les calculs dans une file Redis (RQ) `athena`, traitée par un worker.
- Les calculs sont faits avec DuckDB sur des fichiers Parquet, dans
  `/data/snapshots/<date>/` (snapshot choisi par `SNAPSHOT` dans
  `project/server/main/config.py`).
- Tous les paramètres de la méthode sont dans `project/server/main/config.py`.

### Étapes

| # | Fonction | Rôle |
|---|---|---|
| 0 | `referentiel.build_referentiel()` | référentiel Paysage / scanR : noms, sigles, catégories des établissements |
| 1–2 | `extract_works.extract_snapshot(date)` | extraction du snapshot OpenAlex (articles, institutions, revues) |
| 3 | `disciplines.build_disciplines()` | topics OpenAlex → disciplines ERC |
| 4 | `top_journals.compute_top_journals()` | revues de tête par discipline (SJR) |
| 5 | `top10_citations.compute_top10()` | top 10 % des articles les plus cités |
| 6 | `indicators.compute_indicators()` | effectifs par entité, année et discipline |
| 6b | `copublications.compute_copublications()` | partenaires de chaque pays et établissement |
| 6c | `erc.compute_erc()` | bourses ERC (participations Horizon Europe de l'open data ESR, lues par `horizon.py`) : candidatures et lauréats par établissement, pays et panel |
| 6d | `prix.compute_prix()` | prix et distinctions (lauréats recensés dans Paysage : IUF, médailles du CNRS…) par établissement, année et prix |
| 6e | `publications_auteurs.extraire_publications()` | une seule relecture des works du snapshot pour les deux étapes suivantes : publications des lauréats de prix et signatures des viviers (reprise séparée pour chacune) |
| 6f | `prix_disciplines.compute_prix_disciplines()` | panel ERC des lauréats par leurs publications OpenAlex (ORCID, sinon nom + établissement) |
| 6g | `viviers.compute_viviers()` | viviers de chercheurs actifs par pays (France + pays des comparaisons ERC et bibliométrique), établissement et discipline (auteurs OpenAlex ; articles de revues, plus livres et chapitres en SHS), avec indicateur top 10 % : taux de candidature ERC de la France comparé aux autres pays. Discipline : même règle et même code que les prix (`poids_disciplines`) |
| 7 | `publish.publish()` / `publish.publish_mongo()` | fichiers de publication, puis chargement dans MongoDB |
| 8 | `export_dashboard.build_dashboard()` | aperçu HTML autonome |

Les étapes lancées par `POST /compute` se choisissent dans le corps JSON (liste et
ordre : `ETAPES` dans `project/server/main/tasks.py`, ou `GET /compute`). `pipeline.py`
reste utilisable en Python (voir sa docstring).

## Variables d'environnement

| Variable | Usage | Nécessaire pour |
|---|---|---|
| `APP_SETTINGS` | classe de configuration Flask, ex. `project.server.config.DevelopmentConfig` | toujours |
| `OPENALEX_API_KEY` | clé de l'offre payante OpenAlex, échangée contre des identifiants S3 temporaires | extraction des snapshots quotidiens (étapes 1–2) |
| `MONGO_URI` | connexion à la base MongoDB de tableaux | `publish_mongo()` |
| `MONGO_DB_NAME` | nom de la base (défaut : `tableaux-staging`) | `publish_mongo()` |
| `ODS_API_KEY` | clé de l'open data ESR (data.enseignementsup-recherche.gouv.fr) | `erc.compute_erc()` : obligatoire pour le jeu des propositions soumises (non public) ; `prix.compute_prix()` (jeu des lauréats de prix, non public) |

Les secrets ne sont jamais écrits dans le code ni dans l'image. Dans le
conteneur, le profil AWS `openalex` (`docker/aws_config`) permet aussi
d'explorer le bucket :

```bash
aws s3 ls s3://openalex-snapshots/full/ --profile openalex
```

## Lancement

```bash
make install                   # dépendances Python
python manage.py run_worker    # worker RQ (Redis attendu sur redis://redis:6379/0)
```

Lancer un calcul et suivre son état :

```bash
curl http://<hôte>/compute                     # étapes disponibles
curl -X POST http://<hôte>/compute -d '{}'     # toute la chaîne (sans contrôles ni Mongo)
curl -X POST http://<hôte>/compute -d '{"steps": ["publications", "prix_disciplines", "viviers", "publish"]}'
curl -X POST http://<hôte>/compute -d '{"start": "indicators", "end": "publish"}'
curl -X POST http://<hôte>/compute -d '{"steps": ["viviers"], "params": {"viviers": {"annee_debut": 2016, "out_suffix": "_2016"}}}'
curl -X POST http://<hôte>/compute -d '{"steps": ["publish_mongo"]}'
curl http://<hôte>/tasks/<task_id>
```

Fichier facultatif de corrections du sélecteur d'établissements : copier
`config/etablissements_dashboard.csv.example` en `config/etablissements_dashboard.csv`.

## Image Docker

```bash
make docker-build    # ghcr.io/dataesr/athena:<version> et :latest
make docker-push
make release VERSION=x.y.z   # puis git push origin main --tags
```

Un tag `x.y.z` poussé sur GitHub déclenche aussi la construction et la
publication de l'image `ghcr.io/dataesr/athena` (`.github/workflows/docker-image.yml`).

## Données de référence incluses

- `topics/openalex_topics_erc_panels.json` : correspondance topics OpenAlex → panels ERC
- `scimago/scimagojr_2025.csv` : classement SJR des revues
