# Contrat de données — bibliométrie et bourses ERC (application tableaux)

Ce document décrit les données produites par la chaîne Python (`project/server/main/`)
pour l'application [tableaux](https://github.com/dataesr/tableaux). Il fait foi pour
les noms de champs : ce sont les mêmes dans les fichiers Parquet, dans la base
MongoDB et dans le code du front.

## 1. Règle de nommage

- Noms en français, en `snake_case`, sans abréviation.
- `nb_…` : un nombre d'articles (entier, sauf le top 10 %, en équivalent-articles décimaux).
- `part_…` : un ratio entre 0 et 1, **jamais stocké** (calculé à la lecture, voir §4).
- Les étapes 1 et 2 (extraction OpenAlex) gardent les noms d'OpenAlex (`work_id`,
  `year`, `inst_id`…). Les données publiées n'en contiennent aucun.

| Terme | Sens |
|---|---|
| `entite_id` | ROR court (`02feahw73`), code ISO du pays (`FR`), ou `MONDE` |
| `entite_type` | `monde`, `pays` ou `etablissement` |
| `perimetre` | ensemble d'articles de référence (« le monde ») : `toutes_revues_openalex` ou `revues_sjr` |
| `classement` | classement des disciplines (`erc` aujourd'hui) |
| `annee_publication` | année de parution de l'article |
| `discipline_code` | `LS1`…`LS9`, `PE1`…`PE11`, `SH1`…`SH8` |
| `domaine_code` | `LS`, `PE`, `SH` |

## 2. Les collections

Dans Mongo, chaque collection est chargée sous le nom `<collection>_<horodatage>`
(convention de tableaux) et la version est ajoutée au document du tableau dans
`boards` (`id: "bibliometrie"`, entrée `data[].id = <collection>`).
Fichiers : `<DATA_DIR>/publication/erc/<collection>.jsonl.gz` et `.parquet`,
plus `manifest.json`.

### `entites` — commune à tous les onglets

Une ligne par entité. C'est la clé de jointure de tous les onglets (bibliométrie,
financements, innovation) : toujours `entite_id`.

| Champ | Type | Contenu |
|---|---|---|
| `entite_id` | texte | ROR court, code ISO ou `MONDE` |
| `entite_type` | texte | `monde`, `pays`, `etablissement` |
| `nom` | texte | nom affiché : fichier de corrections > référentiel (Paysage ou scanR) > OpenAlex |
| `nom_openalex` | texte | nom dans OpenAlex |
| `sigle` | texte | sigle du référentiel |
| `categorie` | texte | catégorie du référentiel (Université, Organisme de recherche…) |
| `ville` | texte | ville du référentiel |
| `referentiel_id` | texte | identifiant Paysage (ou scanR) |
| `pays_code` | texte | pays de l'établissement (ou du pays lui-même) |
| `type_openalex` | texte | `education`, `facility`, `government`… |
| `nb_articles` | entier | taille : articles du premier périmètre, toute la période (0 pour une entité présente seulement dans les données ERC) |
| `nb_candidatures_erc` | entier ou null | candidatures ERC, toutes années (null : aucune) |
| `pays_compare` | booléen | pays retenu pour la comparaison internationale |
| `tutelles` | liste | ROR des tutelles (lignée OpenAlex) |
| `nb_structures_rattachees` | entier | structures dont les articles comptent pour l'entité |
| `alias` | liste | termes de recherche : sigles OpenAlex, sigle calculé, alias du fichier de configuration |
| `recherche` | texte | nom + ROR + alias + ville, minuscules, sans accents : filtre « contient » côté front |
| `selecteur` | booléen | proposé dans le sélecteur principal : périmètre du référentiel (structures de tête actives des catégories retenues) qui ont des articles, corrigé par le fichier `config/etablissements_dashboard.csv`. Les entités présentes seulement dans les données ERC n'y sont jamais. |
| `ordre_selecteur` | entier ou null | ordre quand la recherche est vide (France = -1) |

### `biblio_effectifs` — la table principale

Une ligne par `perimetre` × `entite_id` × `annee_publication` × `discipline_code`.
L'entité `MONDE` donne les dénominateurs.

| Champ | Type | Contenu |
|---|---|---|
| `perimetre`, `classement`, `entite_type`, `entite_id`, `annee_publication`, `discipline_code` | | clés |
| `nb_articles` | entier | articles de l'entité |
| `nb_articles_top10_cites` | décimal | articles dans le top 10 % des plus cités de la discipline et de l'année, en équivalent-articles (méthode de Waltman et Schreiber) |
| `nb_articles_revues_tete_disciplinaires` | entier | articles parus dans une des 10 revues de tête de leur discipline |
| `nb_articles_revues_tete_multidisciplinaires` | entier | articles parus dans une revue multidisciplinaire de tête (Nature, Science…) |
| `influence_provisoire` | booléen | fenêtre de citation pas encore close |

Index : `(entite_id, perimetre, classement, annee_publication, discipline_code)`.

### `biblio_copublications`

Une ligne par `perimetre` × `entite_id` × `partenaire_id` × `annee_publication` × `discipline_code`.

| Champ | Type | Contenu |
|---|---|---|
| `entite_type`, `entite_id` | | entité (pays retenu ou établissement) |
| `partenaire_type`, `partenaire_id` | | `pays` (code ISO) ou `etablissement` (ROR) |
| `nb_articles_copublies` | entier | articles cosignés par l'entité et le partenaire |
| `nb_articles_copublies_top10_cites` | décimal | dont dans le top 10 % des plus cités |

Règles : pour un établissement, son propre pays n'est pas un partenaire. Deux
établissements ne sont partenaires sur un article que si chacun y a une affiliation
propre : une UMR en cotutelle ne crée pas de copublication entre ses tutelles. On
garde, par entité et par type de partenaire, les 50 partenaires les plus fréquents.

### `biblio_disciplines`

`classement`, `discipline_code`, `discipline_nom`, `domaine_code`, `domaine_nom`.

### `biblio_revues_tete`

`classement`, `discipline_code`, `categorie` (`disciplinaire` | `multidisciplinaire`),
`rang`, `sjr_id`, `titre`, `issn`, `sjr`, `quartile`, `nb_articles_discipline`,
`part_discipline` (part des articles de la revue qui relèvent de la discipline).

### `biblio_seuils_top10`

Une ligne par `perimetre` × `discipline_code` × `annee_publication` : `nb_articles`,
`nb_articles_top10_cites` (= 10 % de `nb_articles`), `seuil_nb_citations` (citations
minimales pour entrer dans le top), `nb_articles_au_seuil` (ex aequo au seuil),
`fraction_au_seuil` (part de place de chacun), `mode_citations` (`fenetre` | `cumul`),
`influence_provisoire`.

### `biblio_meta` — un seul document

`classement`, `snapshot_openalex` (`source`, `date`), `date_calcul`, `perimetres`,
`annees`, `annees_influence_provisoire`, `nb_revues_tete`, `revues_multidisciplinaires`
(titres), `pays_compares`, `parametres` (paramètres de chaque étape), `methode`
(textes de la note méthodologique, à afficher tels quels), `sources` (mention des sources).

### `erc_effectifs` — bourses ERC

Une ligne par `entite_type` × `entite_id` × `annee_appel` × `discipline_code` × `type_bourse`.
Source : open data ESR, jeu `fr-esr-erc-persons` (`erc.compute_erc()`). L'entité `MONDE`
(toutes les candidatures ERC) donne les dénominateurs des parts.

| Champ | Type | Contenu |
|---|---|---|
| `entite_type`, `entite_id` | | `monde`, `pays` (ISO2, outre-mer sous `FR`), `etablissement` (ROR court) |
| `annee_appel` | entier | année de l'appel ERC |
| `discipline_code` | texte ou null | panel ERC, mêmes codes que `biblio_disciplines` ; null pour les Proof of Concept |
| `type_bourse` | texte | `STG`, `COG`, `ADG`, `SYG`, `POC` |
| `nb_candidatures` | entier | projets déposés (évalués ou financés) |
| `nb_candidatures_retenues` | entier | dont projets financés |

Règles : une candidature est un projet. Elle compte pour la structure d'accueil **au dépôt**
(un lauréat qui change d'établissement avant la signature reste compté où il a candidaté :
le numérateur du taux de succès est toujours inclus dans son dénominateur), et pour les
tutelles OpenAlex de cette structure, comme les articles. Le pays est celui de la structure
d'accueil, pas la nationalité du porteur. Comptage entier : une Synergy à plusieurs hôtes
compte pour chacun. Les structures sans ROR (ni directement, ni via Paysage) ne comptent que
pour leur pays et pour `MONDE` ; elles sont listées dans `erc/erc_non_rattaches.csv`.

Index : `(entite_id, annee_appel, discipline_code, type_bourse)`, `(entite_type, annee_appel)`.

### `erc_meta` — un seul document

`source` (`dataset`, `date_extraction`), `date_calcul`, `annees`, `types_bourse`, `panels`,
`pays_compares` (pays de la comparaison internationale ERC : France puis les pays qui déposent
le plus ; distincts des pays de la bibliométrie, où figurent les États-Unis ou la Chine),
`parametres`, `controles` (taux de rattachement à un ROR, valeurs de `stage` et de `role`),
`methode`, `sources`.

## 3. Indicateurs

| Indicateur | Nom du ratio | Numérateur (entité) | Dénominateur (`MONDE`) |
|---|---|---|---|
| Poids | `part_articles` | `nb_articles` | `nb_articles` |
| Influence | `part_top10_cites` | `nb_articles_top10_cites` | `nb_articles_top10_cites` |
| Prestige | `part_revues_tete` | `nb_articles_revues_tete_disciplinaires` (+ `…_multidisciplinaires` si l'option est cochée) | idem |
| Part d'un partenaire | `part_copublications` | `nb_articles_copublies` (entité, partenaire) | `nb_articles` de l'entité |
| ERC : taux de succès | `taux_succes` | `nb_candidatures_retenues` (entité) | `nb_candidatures` (même entité) |
| ERC : part des candidatures | `part_candidatures` | `nb_candidatures` (entité) | `nb_candidatures` (`MONDE`) |
| ERC : part des lauréats | `part_laureats` | `nb_candidatures_retenues` (entité) | `nb_candidatures_retenues` (`MONDE`) |

## 4. Règle de calcul (à respecter côté serveur)

**On additionne d'abord, on divise ensuite.** Pour une période ou un groupe de
disciplines : sommer les effectifs de l'entité, sommer ceux de `MONDE` (même
périmètre, mêmes années, mêmes disciplines), puis diviser. Ne jamais moyenner des parts.

Exemple de requête Mongo (influence de la France, 2015–2022, toutes disciplines,
périmètre SJR) :

```js
db.biblio_effectifs_<version>.aggregate([
  { $match: { entite_id: { $in: ["FR", "MONDE"] }, perimetre: "revues_sjr",
              classement: "erc", annee_publication: { $gte: 2015, $lte: 2022 } } },
  { $group: { _id: "$entite_id",
              nb_articles_top10_cites: { $sum: "$nb_articles_top10_cites" } } }
])
// part_top10_cites = FR.nb_articles_top10_cites / MONDE.nb_articles_top10_cites
```

Les réponses des routes renvoient chaque ratio avec son numérateur et son
dénominateur, pour que le front affiche le calcul sans rien recalculer :

```json
{ "influence": { "valeur": 0.0541, "numerateur": 177922.1, "denominateur": 3288545.4 } }
```

Onglet ERC, mêmes filtres que la bibliométrie (établissement, `discipline_code`, période) :

- **Profil de l'établissement** : `$match` sur `entite_id` = ROR et les filtres, `$group`
  par `annee_appel` (évolution) ou `discipline_code` (profil) ; `taux_succes` par groupe.
- **Comparaison des établissements** : `$match` sur `entite_type: "etablissement"` et les
  filtres, `$group` par `entite_id`, joindre `entites` sur `entite_id` et garder
  `pays_code = "FR"` ; classer par `nb_candidatures_retenues` ou `taux_succes`. Prévoir un
  seuil minimal de candidatures avant de classer sur un taux (petits effectifs).
- **France et autres pays** : `$match` sur `entite_id` dans `erc_meta.pays_compares` + `MONDE`,
  `$group` par `entite_id` ; le taux de `MONDE` sert de référence.

```js
// Taux de succès de Sorbonne Université, panels PE, appels 2021-2025
db.erc_effectifs_<version>.aggregate([
  { $match: { entite_id: "02en5vm52", annee_appel: { $gte: 2021, $lte: 2025 },
              discipline_code: { $regex: "^PE" } } },
  { $group: { _id: null, nb_candidatures: { $sum: "$nb_candidatures" },
              nb_candidatures_retenues: { $sum: "$nb_candidatures_retenues" } } }
])
// taux_succes = nb_candidatures_retenues / nb_candidatures
```

## 5. Production

| Étape | Fonction | Sortie |
|---|---|---|
| 0 | `referentiel.build_referentiel()` | `/data/referentiel/paysage.parquet` |
| 3 | `disciplines.build_disciplines()` | `disciplines/erc.parquet` |
| 4 | `top10_citations.compute_top10()` | `top10/erc_<perimetre>.parquet`, `…_seuils.parquet` |
| 5 | `top_journals.compute_top_journals()` | `revues_tete/erc.parquet`, `sources_sjr.parquet` |
| 6 | `indicators.compute_indicators()` | `indicateurs/erc_effectifs.parquet`, `erc_entites.parquet` |
| 6b | `copublications.compute_copublications()` | `indicateurs/erc_copublications.parquet` |
| ERC | `erc.compute_erc()` | `erc/erc_effectifs.parquet`, `erc_entites.parquet`, `erc_meta.json`, `erc_non_rattaches.csv` |
| 7 | `publish.publish()` puis `publish.publish_mongo()` | `publication/erc/*.jsonl.gz` puis MongoDB |
