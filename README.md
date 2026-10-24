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
| 7 | `publish.publish()` / `publish.publish_mongo()` | fichiers de publication, puis chargement dans MongoDB |
| 8 | `export_dashboard.build_dashboard()` | aperçu HTML autonome |

Les étapes lancées par `POST /compute` sont choisies dans
`project/server/main/tasks.py` (`create_task_compute`). `pipeline.py` permet de
les enchaîner ou de n'en relancer qu'une partie (voir sa docstring).

## Variables d'environnement

| Variable | Usage | Nécessaire pour |
|---|---|---|
| `APP_SETTINGS` | classe de configuration Flask, ex. `project.server.config.DevelopmentConfig` | toujours |
| `OPENALEX_API_KEY` | clé de l'offre payante OpenAlex, échangée contre des identifiants S3 temporaires | extraction des snapshots quotidiens (étapes 1–2) |
| `MONGO_URI` | connexion à la base MongoDB de tableaux | `publish_mongo()` |
| `MONGO_DB_NAME` | nom de la base (défaut : `tableaux-staging`) | `publish_mongo()` |

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
curl -X POST http://<hôte>/compute -d '{}'
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
