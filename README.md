# Tableau de bord de l’excellence scientifique — onglet Bibliométrie

Tableau de bord au Système de design de l’État (DSFR 1.15), livré sous forme d’**un seul fichier HTML autonome** : aucun appel réseau à l’ouverture, données chiffrées, accès par identifiant / mot de passe.

```
config/        axes.json (référentiel disciplinaire ↔ OpenAlex), etablissements.csv, params.json
pipeline/      extract_openalex.py, build_dashboard.py, demo_data.py, openalex.py, referentiel.py, sjr.py
web/           index.html, app.js, app.css, vendor/dsfr (DSFR intégré au build)
dist/          fichiers HTML générés
```

## 1. Démarrage rapide (données de démonstration)

```bash
pip install -r requirements.txt
python pipeline/demo_data.py                      # data/biblio_demo.json (valeurs fictives)
python pipeline/build_dashboard.py --data data/biblio_demo.json --user demo
```
Ouvrir `dist/tableau-de-bord.html` dans un navigateur récent (double-clic ou hébergement https).

## 2. Données réelles

```bash
export OPENALEX_API_KEY="votre-cle"               # https://openalex.org/settings/api
export OPENALEX_MAILTO="prenom.nom@exemple.gouv.fr"
python pipeline/extract_openalex.py --dry-run     # nombre de requêtes à prévoir
python pipeline/extract_openalex.py              # à relancer chaque jour jusqu’à « ok »
python pipeline/build_dashboard.py --data data/biblio.json --user dgri --user cabinet
```

- **Sobriété (la clé est partagée)** : le budget OpenAlex est celui de la clé, commun à toutes les applications qui l'utilisent.
  - Par défaut : 4 requêtes simultanées et 5 req/s (`--workers`, `--rps`, plafond dur 50 req/s).
  - L'extraction ne consomme qu'une part du budget restant du jour (`--part-budget`, 0.5 par défaut). Toutes les 250 requêtes, elle vérifie que la réserve laissée aux autres applications est intacte, et s'arrête proprement sinon.
  - En cas de 429 : recul exponentiel conforme à la documentation OpenAlex (≈ 5 à 8 essais), simultanéité divisée par 2, respect de `Retry-After`. Un 429 `not_enough_credits` arrête l'extraction immédiatement. Le solde prépayé n'est utilisé qu'avec `--autoriser-prepaye`.
- **Volume** : ~116 entités × 66 disciplines × 3 indicateurs ≈ 30 000 requêtes, soit environ 10 000 par jour avec la clé gratuite (1 $/jour) : comptez 3 jours, en relançant la même commande chaque jour. `--dry-run` affiche le reste à faire et le budget disponible. Avec un solde prépayé, l'extraction complète coûte environ 3 $ et dure une vingtaine de minutes. Toutes les réponses sont mises en cache (`data/cache/`) : rien n'est re-consommé à la reprise.
- **SJR** : Scimago bloque les téléchargements automatisés. Exportez une fois le classement complet depuis https://www.scimagojr.com/journalrank.php (bouton « Download data »), puis déposez le fichier `scimagojr AAAA.csv` dans `data/sjr/`, ou passez-le avec `--sjr-fichier`. Les catégories de chaque revue sont rattachées aux sous-domaines OpenAlex par leur nom. Les catégories non rattachées sont listées dans `data/sjr/categories_non_rattachees.csv` et peuvent être rattachées à la main dans `config/sjr_alias.json`.
- **À vérifier après la 1re extraction** :
  - `data/etablissements_resolus.csv` : identifiants OpenAlex trouvés par recherche de nom. Corrigez les erreurs en renseignant la colonne `openalex_id` de `config/etablissements.csv`.
  - `data/referentiel_topics.csv` : rattachement des ~4 500 topics OpenAlex aux 57 axes. À faire valider par des experts disciplinaires ; les ajustements se font dans `config/axes.json`.

### Cache et rafraîchissement

Chaque réponse est conservée dans `data/cache/`, rangée par périmètre : `comptages` (requêtes de publications, l'essentiel du volume), `referentiel` (topics OpenAlex), `etablissements` (identifiants résolus) et `sjr`. Une relance ne re-télécharge rien de ce qui est déjà en cache.

```bash
python pipeline/extract_openalex.py --cache-info              # état du cache par périmètre
python pipeline/extract_openalex.py --refresh                 # tout re-télécharger
python pipeline/extract_openalex.py --refresh comptages       # seulement les comptages
python pipeline/extract_openalex.py --refresh comptages sjr   # plusieurs périmètres
python pipeline/extract_openalex.py --max-age-jours 30        # ce qui a plus de 30 jours
```

Le rafraîchissement ne vide pas le cache : il marque les anciennes réponses comme périmées, et chacune est remplacée au moment où la nouvelle arrive.
- **Interruption** (coupure, plafond `--max-calls`) : relancez **sans** `--refresh` pour reprendre ; seules les réponses encore périmées sont re-téléchargées.
- **Échec du téléchargement SJR** : l'ancienne version des fichiers est conservée.

Les citations évoluent en continu dans OpenAlex, donc l'appartenance au top 10 % aussi : un rafraîchissement des comptages tous les 3 à 6 mois est raisonnable. Avec une clé premium, `--refresh comptages` prend une dizaine de minutes.

### Revues de référence

Pour chaque discipline, le pipeline retient les 10 revues les mieux classées par le SJR, puis retrouve leur fiche OpenAlex par ISSN. Le tableau de bord affiche ces listes avec un lien vers chaque fiche, les catégories SJR utilisées, et signale les revues introuvables par ISSN : leurs publications sont probablement absentes des comptages, il faut les vérifier. Les listes s'exportent en CSV, pour une discipline ou pour toutes.

### Comparaisons internationales

- **Panel Europe** (`panel_europe` dans `config/params.json`) : Allemagne, Royaume-Uni, Italie, Espagne, Pays-Bas, Suisse, Suède, Belgique, Danemark, Autriche. Ils servent aux rangs et aux rubans de la comparaison internationale.
- **Panel monde** : les 20 premiers pays producteurs sur les 3 dernières années (méthode OST). Il est calculé à la première extraction puis figé dans `config/panel_monde.json` ; pour le recalculer, utilisez `--recalculer-panel-monde`.
- **Fenêtre du top 10 %** : arrêtée à `annee_fin_citations` (2023), car les citations des articles plus récents sont encore incomplètes.
- **Très grandes collaborations** : renseigner `"max_auteurs": 100` exclut de tous les comptages les articles de plus de 100 auteurs. Le changement entraîne une ré-extraction complète, avec de nouvelles requêtes.
- Les comptes sont entiers : seuls les écarts et les rangs entre pays sont interprétables, pas les niveaux absolus (voir la méthodologie du tableau de bord, § 7).

## 3. Comptes et chiffrement

`build_dashboard.py` chiffre les données (AES-256-GCM) avec une clé aléatoire, elle-même chiffrée pour chaque compte à partir de son mot de passe (PBKDF2-SHA256, 600 000 itérations). Le HTML ne contient ni mot de passe ni empreinte exploitable : sans mot de passe valide, il est illisible.

Comptes : `--user login` (mot de passe demandé au clavier), `--users-file comptes.csv` (`login;mot_de_passe`, à ne jamais versionner) ou variable `DASH_USERS="login1:mdp1;login2:mdp2"`. Ajouter ou retirer un compte = régénérer le fichier.

Limites : la protection repose entièrement sur la robustesse des mots de passe (phrases de passe de 12+ caractères). Un compte retiré garde l’accès aux anciennes copies du fichier. Pour une diffusion large, placer le fichier derrière une authentification serveur (ProConnect, SSO ministériel).

## 4. Méthode (résumé)

Deux indicateurs, tous deux des **parts mondiales**, calculés pour chacun des **38 axes disciplinaires** :

| Indicateur | Définition |
|---|---|
| Part du top 10 % mondial | Articles de l’entité parmi les 10 % les plus cités au monde (même année, même sous-domaine : `citation_normalized_percentile`) ÷ ensemble des articles mondiaux de ce top 10 % |
| Part des revues de tête | Articles de l’entité parus dans les 10 meilleures revues SJR de l’axe ÷ ensemble des articles mondiaux parus dans ces revues |
| Évolution P1 → P2 | Part sur la 2e moitié de la période ÷ part sur la 1re moitié − 1 |

- **Agrégats** : une macro-discipline (A à G) ou « toutes disciplines » additionne les comptes de ses axes, au numérateur comme au dénominateur mondial. Il n’y a pas de « top 10 revues global » : chaque axe a ses 10 revues.
- Les 19 axes transverses (H) sont calculés de la même façon (avec leurs propres 10 revues SJR), mais présentés à part : ils recoupent les axes disciplinaires, donc ils ne sont ni additionnés ni inclus dans les macro-disciplines ou « toutes disciplines ».
- **Matrice** (38 axes disciplinaires, ou 19 axes transverses) : en abscisse la part (échelle log), en ordonnée son évolution P1 → P2. Les deux axes de lecture se croisent sur la valeur toutes disciplines de l’entité.

Le tableau de bord liste, dans sa méthodologie, tous les topics rattachés à chaque axe (avec recherche et export CSV). Pour des données extraites avant cette fonctionnalité, `build_dashboard.py` reconstruit cette liste sans requête, à partir de `data/cache/topics.json` et `config/axes.json`.

Les disciplines sont reconstruites à partir du topic principal OpenAlex (sous-domaines ASJC, identiques aux catégories SJR). Les régions agrègent leurs établissements sans double compte ; les organismes nationaux ne sont rattachés à aucune région.
