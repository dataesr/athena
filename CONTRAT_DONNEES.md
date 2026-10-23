# Contrat de données — bibliométrie (application tableaux)

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
| `nb_articles` | entier | taille : articles du premier périmètre, toute la période |
| `pays_compare` | booléen | pays retenu pour la comparaison internationale |
| `tutelles` | liste | ROR des tutelles (lignée OpenAlex) |
| `nb_structures_rattachees` | entier | structures dont les articles comptent pour l'entité |
| `alias` | liste | termes de recherche : sigles OpenAlex, sigle calculé, alias du fichier de configuration |
| `recherche` | texte | nom + ROR + alias + ville, minuscules, sans accents : filtre « contient » côté front |
| `selecteur` | booléen | proposé dans le sélecteur principal : périmètre du référentiel (structures de tête actives des catégories retenues) qui ont des articles, corrigé par le fichier `config/etablissements_dashboard.csv` |
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

## 3. Indicateurs

| Indicateur | Nom du ratio | Numérateur (entité) | Dénominateur (`MONDE`) |
|---|---|---|---|
| Poids | `part_articles` | `nb_articles` | `nb_articles` |
| Influence | `part_top10_cites` | `nb_articles_top10_cites` | `nb_articles_top10_cites` |
| Prestige | `part_revues_tete` | `nb_articles_revues_tete_disciplinaires` (+ `…_multidisciplinaires` si l'option est cochée) | idem |
| Part d'un partenaire | `part_copublications` | `nb_articles_copublies` (entité, partenaire) | `nb_articles` de l'entité |

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

## 5. Production

| Étape | Fonction | Sortie |
|---|---|---|
| 0 | `referentiel.build_referentiel()` | `/data/referentiel/paysage.parquet` |
| 3 | `disciplines.build_disciplines()` | `disciplines/erc.parquet` |
| 4 | `top10_citations.compute_top10()` | `top10/erc_<perimetre>.parquet`, `…_seuils.parquet` |
| 5 | `top_journals.compute_top_journals()` | `revues_tete/erc.parquet`, `sources_sjr.parquet` |
| 6 | `indicators.compute_indicators()` | `indicateurs/erc_effectifs.parquet`, `erc_entites.parquet` |
| 6b | `copublications.compute_copublications()` | `indicateurs/erc_copublications.parquet` |
| 7 | `publish.publish()` puis `publish.publish_mongo()` | `publication/erc/*.jsonl.gz` puis MongoDB |
