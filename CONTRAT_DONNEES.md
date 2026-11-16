# Contrat de données — bibliométrie, bourses ERC, prix et viviers (application tableaux)

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
`boards` (`id: "suivi-recherche"`, `TABLEAUX_BOARD_ID` dans `config.py` ; entrée
`data[].id = <collection>`). `publish_mongo()` ajoute la version à `data[].versions` sans
changer la version active : elle s'active dans l'administration du tableau
(`/admin/suivi-recherche`).
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
| `groupe_etablissement` | texte ou null | groupe de comparaison des établissements français : `universites`, `organismes`, `sante`, `ecoles-ingenieurs`, `grands-etablissements`, `commerce-iep`, `prive`, `autres` (mêmes codes que `GROUPES_ETABLISSEMENTS` de tableaux). Déduit de **toutes** les catégories Paysage (typologie universitaire, habilitation CTI…), pas seulement de `categorie`, qui est souvent un statut (« Grand établissement » pour l'Université de Lorraine) ; la colonne `groupe` du fichier de corrections l'emporte. null hors établissements français. À utiliser à la place de `categorie` pour regrouper les établissements comparés |
| `ville` | texte | ville du référentiel |
| `referentiel_id` | texte | identifiant Paysage (ou scanR) |
| `pays_code` | texte | pays de l'établissement (ou du pays lui-même) |
| `type_openalex` | texte | `education`, `facility`, `government`… |
| `nb_articles` | entier | taille : articles du premier périmètre, toute la période (0 pour une entité présente seulement dans les données ERC) |
| `nb_candidatures_erc` | entier ou null | candidatures ERC, toutes années (null : aucune) |
| `nb_laureats_prix` | entier ou null | lauréats de prix (`prix_effectifs`, tous prix, toutes années ; null : aucun). Aucune entité n'est ajoutée au registre par les prix |
| `pays_compare` | booléen | pays retenu pour la comparaison internationale **bibliométrique** (`biblio_meta.pays_compares`) |
| `pays_compare_erc` | booléen | pays retenu pour la comparaison internationale **ERC** (`erc_meta.pays_compares`) |
| `tutelles` | liste | ROR des tutelles (lignée OpenAlex) |
| `nb_structures_rattachees` | entier | structures dont les articles comptent pour l'entité |
| `alias` | liste | termes de recherche : sigles OpenAlex, sigle calculé, alias du fichier de configuration |
| `recherche` | texte | nom + ROR + alias + ville, minuscules, sans accents : filtre « contient » côté front |
| `selecteur` | booléen | proposé dans le sélecteur principal : périmètre du référentiel (structures de tête actives des catégories retenues) qui ont des articles, corrigé par le fichier `config/etablissements_dashboard.csv`. Les entités présentes seulement dans les données ERC n'y sont jamais. |
| `ordre_selecteur` | entier ou null | ordre quand la recherche est vide (France = -1) |

### `biblio_effectifs` — la table principale

Une ligne par `perimetre` × `entite_id` × `annee_publication` × `discipline_code`.
L'entité `MONDE` donne les dénominateurs.

Pays présents (`biblio_meta.pays_effectifs`) : l'**union** des pays de la comparaison
bibliométrique (`biblio_meta.pays_compares`) et de la comparaison ERC
(`erc_meta.pays_compares`), pour les deux périmètres, toutes années et toutes disciplines.
Les pays ERC servent à l'intensité de candidature (§3) ; ils ne deviennent pas des pays de la
comparaison bibliométrique (`entites.pays_compare` inchangé). Pour cela, `erc.compute_erc()`
est lancé avant `indicators.compute_indicators()` ; `publish()` signale tout pays de
`erc_meta.pays_compares` absent des effectifs.

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

Publiée seulement si `PUBLIER_COPUBLICATIONS = True` (config.py, défaut `False`) : sinon absente de `manifest.json` et de Mongo.

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
(titres), `pays_compares` (comparaison bibliométrique), `pays_effectifs` (pays présents dans
`biblio_effectifs` : `pays_compares` + pays de la comparaison ERC), `parametres` (paramètres de chaque étape), `methode`
(textes de la note méthodologique, à afficher tels quels), `sources` (mention des sources).

### `erc_effectifs` — bourses ERC

Une ligne par `entite_type` × `entite_id` × `annee_appel` × `discipline_code` × `type_bourse`.
Source : open data ESR, participations aux projets Horizon Europe (`erc.compute_erc()`, lecture
par `horizon.py`) : propositions évaluées (`fr-esr-horizon-projets-soumis-candidats`, structure
au dépôt) et projets lauréats (`fr-esr-horizon-projects-entities`, structure à la signature),
lignes ERC (`thema_code = "ERC"`) des porteurs seulement (`erc_role = "PI"`). L'entité `MONDE`
(tous les projets ERC) donne les dénominateurs des parts.

Couverture : appels **Horizon Europe (2021 et après)**. Les appels Horizon 2020 ne sont pas
dans ces jeux. Les années dont les résultats ne sont pas tous publiés sont listées dans
`erc_meta.annees_provisoires`.

| Champ | Type | Contenu |
|---|---|---|
| `entite_type`, `entite_id` | | `monde`, `pays` (ISO2, outre-mer sous `FR`), `etablissement` (ROR court) |
| `annee_appel` | entier | année de l'appel ERC |
| `discipline_code` | texte ou null | panel ERC (`panel_code` de la source), mêmes codes que `biblio_disciplines` ; null quand la source n'a pas de panel (voir ci-dessous) |
| `type_bourse` | texte | code ERC du type de bourse (`destination_code`, en majuscules : `SyG` → `SYG`), voir ci-dessous |
| `nb_candidatures` | entier | projets évalués (`project_id` distincts du jeu des soumis) déposés dans l'entité |
| `nb_candidatures_retenues` | entier | dont projets financés (`project_id` présent chez les lauréats), où que le projet soit signé |
| `nb_laureats` | entier | projets financés (`project_id` distincts du jeu des lauréats) **signés** dans l'entité ; pour `MONDE`, chaque projet financé une fois |
| `nb_laureats_arrivees` | entier | lauréats signés dans l'entité, non déposés dans l'entité ; pour `MONDE` : lauréats absents du jeu des soumis (0 attendu) |
| `nb_laureats_departs` | entier | candidatures retenues déposées dans l'entité, signées dans une autre (0 pour `MONDE`) |
| `nb_pi_candidats` | entier | porteurs (lignes `erc_role = "PI"`) des projets évalués, rattachés à l'entité |
| `nb_pi_laureats` | entier | porteurs des projets financés, rattachés à l'entité |

Deux modes de comptage, au même grain : **en projets** (`nb_candidatures`, `nb_laureats`, par
défaut) ou **en porteurs** (`nb_pi_candidats`, `nb_pi_laureats`). Ils ne diffèrent que pour
les Synergy Grants (2 à 4 PI par projet, chacun rattaché à sa structure) : une Synergy à deux
PI d'une même université compte 1 projet et 2 PI pour cette université.

Règle dépôt / signature (celle des statistiques officielles) : le jeu source suit le parcours
du porteur. Un projet financé dont le porteur a changé d'établissement (ou de pays) entre le
dépôt et la signature a sa ligne dans le jeu des soumis dans la structure de dépôt et sa ligne
dans le jeu des lauréats dans la nouvelle structure. **La candidature est comptée dans la structure de
dépôt, le lauréat dans la structure de signature.** Si un porteur dépose dans A et signe dans
B, A compte une candidature, une candidature retenue et un départ ; B compte un lauréat et une
arrivée, sans la candidature correspondante. Pour chaque ligne :
`nb_laureats = nb_candidatures_retenues + nb_laureats_arrivees - nb_laureats_departs`
(vérifié par `compute_erc()`, écarts dans `erc_meta.controles`). Arrivées et départs se
comptent au niveau de l'entité : un passage d'un institut à un autre de la même université
n'est ni une arrivée ni un départ pour l'université. Un projet lauréat absent du jeu des
soumis compte comme arrivée partout, `MONDE` compris (nombre dans `erc_meta.controles`).

Établissements (même logique que scanR, `horizon.rattacher_structures`) : un projet compte
pour la structure participante (`entities_id` : Paysage, ROR ou RNSR) et, pour une participation
française, pour **toutes les tutelles** des laboratoires RNSR de `numero_national_de_structure`
(plusieurs valeurs séparées par `;` ; tutelles « établissement tutelle » du référentiel des
organisations de scanR). Un ERC hébergé par le CNRS dans une UMR compte donc pour le CNRS et pour
les universités et écoles tutelles de l'UMR. Chaque structure est remplacée par son successeur
Paysage (relation « structure-predecesseur », jusqu'à la structure actuelle) avant d'être
convertie en ROR : les projets s'affichent sur les établissements actuels, jamais sur
d'anciens noms ou des établissements fusionnés. Le projet compte enfin pour les tutelles
OpenAlex de ces établissements, comme les articles. Un même projet ne compte qu'une fois par
entité, quel que soit le nombre de chemins qui y mènent.

Autres règles : une candidature est un projet (`project_id`). Le pays est celui de la structure
(dépôt pour les candidatures, signature pour les lauréats), pas la nationalité du porteur.
Comptage entier : une Synergy à plusieurs hôtes compte pour chacun. Les structures sans ROR
(ni directement, ni via Paysage) ne comptent que pour leur pays et pour `MONDE` ; elles sont
listées dans `erc/erc_non_rattaches.csv`.

Types de bourse (libellés et effectifs dans `erc_meta.types_bourse`) :

| `type_bourse` | Libellé | Panel |
|---|---|---|
| `STG` | Starting Grant | oui (quelques candidatures sans panel) |
| `COG` | Consolidator Grant | oui (quelques candidatures sans panel) |
| `ADG` | Advanced Grant | oui (quelques candidatures sans panel) |
| `SYG` | Synergy Grant | non |
| `POC` | Proof of Concept | non |
| `PERA` | Public Engagement with Research Award (prix) | non |
| `SJI` | Science Journalism Initiative (pas de ligne PI chez les lauréats) | non |
| `ERC-OTHER` | Autres actions ERC (pas de ligne PI chez les lauréats) | non |

Seuls les types qui ont des lignes `erc_role = "PI"` apparaissent dans `erc_effectifs`.

Tous les types sont gardés dans les données. Le calcul par défaut porte sur
`erc_meta.types_bourse_defaut` (`STG`, `COG`, `ADG`) : les autres n'ont pas de panel et, pour
PERA, SJI et ERC-OTHER, ne sont pas des bourses de recherche.

`discipline_code` est null quand le projet n'a pas de panel dans la source : toujours pour
les types sans panel (`avec_panel: false` dans `erc_meta.types_bourse`), et pour quelques
STG / COG / ADG (`nb_candidatures_sans_panel`). Ces lignes comptent dans « toutes
disciplines » mais dans aucun panel : la somme des panels peut donc être un peu inférieure
au total.

Index : `(entite_id, annee_appel, discipline_code, type_bourse)`, `(entite_type, annee_appel)`.

### `erc_meta` — un seul document

| Champ | Contenu |
|---|---|
| `source` | `soumis`, `laureats` (identifiants des jeux), `date_extraction` |
| `date_calcul` | date du calcul |
| `annees` | années d'appel présentes |
| `annees_provisoires` | années d'appel dont les résultats ne sont pas tous publiés : candidatures et taux de succès vont encore évoluer (à signaler, ou exclure des comparaisons dans le temps) |
| `annees_provisoires_motifs` | `{ "2025": "aucune candidature ADG ; …" }` : pourquoi chaque année est provisoire |
| `types_bourse` | liste d'objets `{ code, libelle, avec_panel, principal, nb_candidatures, nb_candidatures_retenues, nb_laureats, nb_pi_candidats, nb_pi_laureats, nb_candidatures_sans_panel }` (`MONDE`) ; `avec_panel` : au moins la moitié des projets du type ont un panel |
| `modes_comptage` | `{ projets: ["nb_candidatures", "nb_laureats"], pi: ["nb_pi_candidats", "nb_pi_laureats"] }` |
| `types_bourse_defaut` | types du calcul par défaut : `["STG", "COG", "ADG"]` |
| `panels` | panels présents |
| `pays_compares` | pays de la comparaison internationale ERC : France puis les 25 pays qui déposent le plus (tous types) ; distincts des pays de la bibliométrie, où figurent les États-Unis ou la Chine |
| `parametres` | paramètres du calcul |
| `controles` | liens vers les établissements par origine (`liens_etablissement` : `structure`, `tutelle_rnsr`), `nb_participations_successeur`, `nb_participations_avec_rnsr`, `nb_participations_avec_tutelles_rnsr` ; nombres de lignes par jeu, valeurs de `stage`, `erc_role` et `destination_code`, taux de rattachement à un ROR (candidatures et lauréats), panels inconnus de la bibliométrie ; mobilité : `nb_laureats_changement_etablissement` (lauréats dont la structure de signature diffère de celle de dépôt), `nb_laureats_changement_pays`, `nb_laureats_sans_candidature` (lauréats absents du jeu des soumis), `nb_lignes_ecart_controle_laureats` (0 attendu) |
| `methode`, `sources` | textes à afficher tels quels (couverture, années provisoires, règles) |

Règle des années provisoires (`ERC_ANNEES_PROVISOIRES` dans `config.py`) : liste imposée si
elle est renseignée ; sinon, détection automatique sur les candidatures `MONDE` des types
principaux. Une année est provisoire si l'un de ces types, présent les autres années, n'y a
aucune candidature ou aucun lauréat, ou si ses candidatures sont inférieures à 75 %
(`ERC_SEUIL_ANNEE_COMPLETE`) de la médiane des autres années.

### `prix_effectifs` — prix et distinctions

Source : jeu ODS `fr_esr_paysage_laureats_all` (non public, clé `ODS_API_KEY`) : lauréats
des prix et distinctions recensés dans Paysage (Institut universitaire de France,
médailles du CNRS, prix des académies et des sociétés savantes…), une ligne par lauréat
d'un prix une année (`prix_laureat_identifiant`). Le recensement n'est pas exhaustif,
surtout pour les années anciennes.

| Champ | Type | Contenu |
|---|---|---|
| `entite_type`, `entite_id` | | `pays` (`FR` : tous les lauréats du jeu) ou `etablissement` (ROR court) |
| `annee` | entier | année d'attribution (`annee` de la source, à défaut les 4 premiers chiffres de `prix_annee`) |
| `prix_id` | texte | identifiant Paysage du prix (`prix_identifiant`), voir `prix_catalogue` |
| `nb_laureats` | entier | lauréats du prix cette année rattachés à l'entité |
| `nb_laureats_femmes`, `nb_laureats_hommes` | entier | dont femmes, dont hommes (`laureat_personne_genre` ; le reste : genre inconnu ou lauréat structure) |

Établissement, même logique que les ERC (scanR) : chaque structure d'affiliation du lauréat
(`laureat_structures_associees_identifiant`, identifiants Paysage, et leurs ROR), remplacée
par son successeur Paysage (établissement actuel : « Université de Provence » compte pour
Aix-Marseille Université), plus toutes les tutelles de ses laboratoires RNSR
(`laureat_structures_associees_identifiant_rnsr`), puis, si `PRIX_RATTACHEMENT_LIGNEE`, les
tutelles OpenAlex. Un lauréat de type `Structure` (ex. entreprise du concours i-Nov) compte
pour la structure elle-même. Les laboratoires qui ont un ROR dans Paysage sont aussi des
entités (comme en bibliométrie) : pour comparer des établissements, ne garder que les
entités du sélecteur (`entites.selecteur`).
Comptage entier : un lauréat compte 1 pour chacun de ses établissements, une seule fois.
On compte des **attributions**, pas des personnes : une personne nommée membre junior puis
senior de l'IUF compte deux fois dans le groupe IUF (sommer les prix d'un groupe reste juste).
Lauréats sans année : écartés (`prix_meta.controles.nb_laureats_sans_annee`).

### `prix_catalogue` — un document par prix

| Champ | Contenu |
|---|---|
| `prix_id` | identifiant Paysage du prix |
| `libelle`, `libelle_francais`, `libelle_anglais` | libellés |
| `categories` | liste (`prix_categories` : « Médaille du CNRS », « Prix de thèse »…) |
| `porteurs_ids`, `porteurs_libelles` | listes : structures qui décernent le prix (Paysage) |
| `site_internet`, `wikidata` | liens |
| `nb_laureats`, `nb_laureats_femmes`, `nb_laureats_hommes` | lauréats, toutes années (`FR`) |
| `annee_min`, `annee_max` | première et dernière année présentes |
| `groupes` | codes des groupes de `prix_meta.groupes` auxquels le prix appartient |

### `prix_laureats` — un document par lauréat

Liste nominative (données ouvertes de Paysage), pour afficher les lauréats d'un
établissement. Publiée si `PRIX_PUBLIER_LAUREATS`.

| Champ | Contenu |
|---|---|
| `laureat_id` | `prix_laureat_identifiant` |
| `prix_id`, `prix_libelle`, `annee` | prix et année |
| `laureat_type` | `Personne` ou `Structure` |
| `personne_id` | identifiant Paysage du lauréat (personne ou structure) |
| `libelle`, `prenom`, `nom`, `genre` | identité (`genre` : `femme`, `homme` ou null) |
| `structures_ids`, `structures_libelles` | listes : affiliations dans la source (les deux listes ne sont **pas** dans le même ordre) |
| `idref`, `orcid`, `wikidata` | identifiants de la personne |
| `entites` | liste des `entite_id` (établissements) pour lesquels le lauréat compte ; filtre `{ entites: "<ROR>" }` |
| `discipline_code`, `domaine_code` | panel ERC principal du lauréat (le plus fréquent dans ses publications) et son domaine (`LS`, `PE`, `SH`) ; null si non classé. Présents si `prix_disciplines.compute_prix_disciplines()` a été lancé |
| `disciplines` | liste `{ discipline_code, poids }` (parts des publications, somme 1 ; vide si non classé) |
| `methode_discipline` | `orcid` (auteur OpenAlex retrouvé par l'ORCID), `nom_affiliation` (même nom et une publication dans l'un des `entites`), `non_classe` |
| `nb_publications_discipline` | publications utilisées (fenêtre de `PRIX_DISC_FENETRE` ans jusqu'au prix ; toutes si moins de `PRIX_DISC_MIN_PUBLICATIONS`) |

### `prix_effectifs_disciplines` — lauréats par discipline

Même grain que `prix_effectifs` plus `discipline_code` (panel ERC, mêmes codes que
`biblio_disciplines` et `erc_effectifs`) : une ligne par `entite_type` × `entite_id` × `annee`
× `prix_id` × `discipline_code`. Seuls les lauréats classés y figurent (couverture :
`prix_meta.disciplines.controles`). Publiée si `compute_prix_disciplines()` a été lancé.

| Champ | Contenu |
|---|---|
| `nb_laureats` | lauréats dont c'est la discipline **principale** (entier ; chaque lauréat classé compte une fois) |
| `nb_laureats_fractionnaire` | somme des poids du lauréat dans cette discipline (décimal) |

Les deux comptages ne se mélangent pas. Sur une même sélection, Σ `nb_laureats` = Σ
`nb_laureats_fractionnaire` = nombre de lauréats classés (≤ `prix_effectifs.nb_laureats`).
Domaine : additionner les panels du domaine (lettres en tête du code).

### `prix_meta` — un seul document

| Champ | Contenu |
|---|---|
| `source` | `jeu`, `date_extraction` |
| `date_calcul` | date du calcul |
| `annees` | années présentes (de la première à la dernière) |
| `groupes` | liste d'objets `{ code, libelle, prix, nb_prix, nb_laureats }` : groupes de prix proposés (`PRIX_GROUPES` dans `config.py` : `IUF` = prix décernés par l'IUF, membres juniors et seniors ; `MEDAILLES_CNRS`) |
| `groupe_defaut` | code du groupe affiché par défaut (`IUF`) |
| `nb_prix` | nombre de prix |
| `parametres` | paramètres du calcul |
| `controles` | `nb_lignes`, `nb_laureats`, `nb_lignes_doublons`, `nb_laureats_sans_annee`, `laureat_type`, `genre`, `nb_personnes_sans_structure`, `nb_laureats_rattaches_etablissement`, `part_laureats_avec_structure_rattaches`, `liens_etablissement` (`structure`, `tutelle_rnsr`), `nb_structures_successeur`, `nb_laureats_avec_rnsr`, `nb_laureats_avec_tutelles_rnsr`, `nb_etablissements`, `groupes_vides` |
| `methode`, `sources` | textes à afficher tels quels (avec, si elles sont calculées, les règles des disciplines) |
| `disciplines` | si `compute_prix_disciplines()` a été lancé : `classement`, `parametres` (`fenetre`, `min_publications`, `noms`, `annee_min_publications`), `controles` (`nb_laureats_personnes`, `nb_avec_orcid`, `par_methode`, `part_classes`, `par_domaine`) |

### `viviers_effectifs` — chercheurs actifs par pays, établissement et discipline

Dénominateur commun aux ERC, aux projets (Horizon Europe, ANR…) et aux prix : candidatures ou
lauréats rapportés au nombre de chercheurs actifs. Publiée si `viviers.compute_viviers()` a été
lancé. Une ligne par `entite_type` × `entite_id` × `discipline_code` ; **pas d'année** : le
vivier est mesuré sur une seule période (`viviers_meta.periode`, 2021-2025 pour le moment : `VIVIER_ANNEE_DEBUT`-`VIVIER_ANNEE_FIN`,
propres aux viviers et changées explicitement).

Un chercheur est un auteur OpenAlex (`author_id`). Pays présents (`viviers_meta.pays`) : la
France, les pays de la comparaison ERC (`erc_meta.pays_compares`, pour le taux de candidature,
§3) et ceux de la comparaison bibliométrique (`biblio_meta.pays_compares`). Seules les
signatures dans ces pays sont lues. Un chercheur est **actif** s'il a au moins `VIVIER_MIN_PUBLICATIONS` (3) publications sur la période,
dont au moins `VIVIER_MIN_PUBLICATIONS_RECENTES` (1) sur les `VIVIER_ANNEES_RECENTES` (2) dernières
années. Publications : les articles de `biblio_effectifs` (périmètre `viviers_meta.perimetre`,
revues, avec discipline), **plus les livres et chapitres d'ouvrages dont le panel est en SHS**
(`VIVIER_LIVRES_TYPES`, `VIVIER_LIVRES_DOMAINES` ; hors SHS, OpenAlex classe beaucoup d'actes
de conférence en chapitres, ex. Springer LNCS). Un livre ou un chapitre compte comme une
publication (activité, discipline, rattachements) mais jamais dans le top 10 %. Les
publications à plus de `VIVIER_MAX_AUTEURS_PAR_ARTICLE` (100) auteurs sont ignorées.

| Champ | Type | Contenu |
|---|---|---|
| `entite_type`, `entite_id` | | `pays` (code ISO2, outre-mer sous `FR`) ou `etablissement` (ROR court, établissements français) |
| `discipline_code` | texte | panel ERC, mêmes codes que `biblio_disciplines` |
| `nb_chercheurs` | entier | chercheurs actifs dont c'est la discipline **principale** |
| `nb_chercheurs_fractionnaire` | décimal | somme des parts de la discipline dans les articles de chaque chercheur |
| `nb_chercheurs_top10` | entier | dont chercheurs « top 10 % » (discipline principale) |
| `nb_chercheurs_top10_fractionnaire` | décimal | idem, en fractionnaire |
| `nb_chercheurs_auteur_principal` | entier | dont premier ou dernier auteur, ou correspondant, d'au moins une publication (discipline principale) |

Pays : un chercheur compte pour un pays si au moins `VIVIER_PART_MIN_PAYS` (50 %) de ses
publications des années récentes y ont une affiliation ; **même règle pour tous les pays, France
comprise** (un chercheur partagé à parts égales entre deux pays compte pour les deux).
Activité et parts se calculent sur toutes ses signatures lues, tous pays confondus.

Établissement : un chercheur compte pour un établissement si au moins
`VIVIER_PART_MIN_ETABLISSEMENT` (50 %) de ses publications des années récentes y sont affiliés,
structure ou l'une de ses tutelles (lignée OpenAlex corrigée, comme les articles) : un chercheur
d'UMR compte pour l'UMR, ses universités et le CNRS ; une mobilité récente le rattache au nouvel
établissement. Seuls les établissements de `entites` (étape 6) sont retenus. Comptage entier :
les établissements ne s'additionnent pas.

Chercheur « top 10 % » : au moins `VIVIER_MIN_TOP10` (1) article de la période dont la
`fraction_top10_cites` (étape 4) atteint `VIVIER_SEUIL_FRACTION_TOP10` (0,5). Les années de
`viviers_meta.annees_influence_provisoire` ont une fenêtre de citation pas encore close.

Les deux comptages (principal / fractionnaire) ne se mélangent pas. Sur une même entité,
Σ `nb_chercheurs` = Σ `nb_chercheurs_fractionnaire` = chercheurs actifs de l'entité.
Domaine : additionner les panels du domaine.

Index : `(entite_id, discipline_code)`, `(entite_type, discipline_code)`.

### `viviers_chercheurs` — un document par chercheur actif rattaché à la France

Liste nominative (données ouvertes OpenAlex) des chercheurs actifs rattachés à `FR` ou à un
établissement français (pas ceux des autres pays). Publiée si `VIVIER_PUBLIER_CHERCHEURS`.

| Champ | Contenu |
|---|---|
| `chercheur_id` | identifiant d'auteur OpenAlex (`A…`) |
| `nom`, `orcid` | nom affiché par OpenAlex (le plus récent), ORCID (null si aucun) |
| `pays` | pays de rattachement, part décroissante (ex. `["FR", "DE"]` pour un chercheur partagé) ; contient `FR` dès que `entites` n'est pas vide (avec les seuils par défaut, `VIVIER_PART_MIN_PAYS` ≤ `VIVIER_PART_MIN_ETABLISSEMENT`) |
| `entites` | liste des `entite_id` (établissements) de rattachement, part décroissante ; filtre `{ entites: "<ROR>" }` ; vide si aucun établissement n'atteint le seuil (le chercheur compte alors pour `FR` seulement) |
| `discipline_code`, `domaine_code` | panel principal et son domaine |
| `disciplines` | liste `{ discipline_code, poids }` (somme 1) |
| `nb_publications`, `nb_publications_recentes` | publications de la période, des années récentes (articles + livres et chapitres SHS) |
| `nb_livres_chapitres` | dont livres et chapitres (SHS) |
| `nb_publications_auteur_principal`, `auteur_principal` | publications en premier ou dernier auteur ou correspondant ; booléen (seuil `VIVIER_MIN_AUTEUR_PRINCIPAL`) |
| `nb_articles_top10`, `nb_articles_top10_cites`, `top10` | articles (seuls les articles ont une référence de citations) comptés dans le top 10 % (entier, seuil de fraction), somme des fractions (décimal), booléen « chercheur top 10 % » |
| `annee_premiere_publication`, `annee_derniere_publication` | dans la période (pas un âge académique) |

Index : `(entites, discipline_code)`, `(chercheur_id)`, `(orcid)`.

### `viviers_meta` — un seul document

`date_calcul`, `classement`, `perimetre`, `pays` (pays présents), `periode` (`annee_debut`, `annee_fin`,
`annee_debut_recente`), `annees_influence_provisoire`, `parametres` (seuils et paramètres
d'extraction), `controles` (`nb_auteurs_fenetre`, `nb_signatures_fenetre`,
`nb_signatures_grandes_collaborations`, `nb_chercheurs_actifs` (tous pays), `nb_chercheurs_france`,
`nb_chercheurs_rattaches_pays`, `par_pays`, `nb_chercheurs_top10`,
`nb_chercheurs_auteur_principal`, `nb_chercheurs_avec_orcid`, `nb_chercheurs_rattaches`,
`part_rattaches`, `nb_etablissements`, `nb_articles_perimetre`, `nb_articles_signes_pays_lus`,
`nb_articles_grandes_collaborations`, `nb_livres_chapitres`, `nb_chercheurs_avec_livres`, `par_domaine`).
`pays_noms` (code → nom français), `pays_sans_comparaison_erc` (pays du vivier absents de
`erc_meta.pays_compares` : pas de taux de candidature ERC pour eux).

**Textes pour le public**, à afficher tels quels : une liste de paragraphes courts, sans nom de
champ, avec les seuils et années tirés de la configuration du calcul.

| Clé | Contenu | Quand l'afficher |
|---|---|---|
| `methode` | liste : publications (le premier), vivier, chercheur actif, pays, établissement, discipline, top 10 %, auteur principal | note de méthode du vivier |
| `lecture` | liste : non-additivité entre établissements et entre pays, total des établissements ≠ France, pas de chiffre par année | partout où le vivier est affiché |
| `limites` | liste des limites **générales** : seuls les publiants, désambiguïsation des auteurs, affiliations | partout où le vivier est affiché |
| `limites_par_usage` | objet `{ cle: [textes] }` : limites propres à un usage | seulement dans le contexte de la clé (voir `limites_usages`) |
| `limites_usages` | objet `{ cle: contexte }` : quand afficher chaque groupe (texte pour les développeurs, pas pour le public) | — |

Clés de `limites_par_usage` :

| Clé | À afficher avec |
|---|---|
| `sciences_humaines_sociales` | des chiffres du domaine SH, ou toutes disciplines confondues |
| `types_bourse_erc` | un taux par type de bourse ERC (Starting, Consolidator, Advanced) |
| `pays_sans_comparaison_erc` | un pays de `pays_sans_comparaison_erc` |

Les règles de calcul pour les développeurs (principal / fractionnaire sans mélange, domaine =
somme des panels, taux annuel) ne figurent pas dans ces textes : voir `viviers_effectifs` et §3.

`publications` : sur quelles publications le vivier est calculé, à afficher ou à reprendre en
note de méthode dans tableaux :

| Champ | Contenu |
|---|---|
| `source`, `snapshot` | `OpenAlex` et date du snapshot |
| `perimetre`, `perimetre_libelle` | `toutes_revues_openalex` (tous les articles de revues d'OpenAlex, défaut) ou `revues_sjr` (revues indexées dans SJR) ; le libellé mentionne les livres et chapitres SHS |
| `revues_sjr_seulement` | booléen |
| `types_documents`, `types_sources` | articles : ex. `["article"]` (reviews exclues), `["journal"]` |
| `annee_debut`, `annee_fin` | période des publications (= `periode`) |
| `max_auteurs_par_article` | au-delà, article ignoré (grandes collaborations) |
| `classement_disciplines`, `discipline_article` | topic principal OpenAlex → panel ERC ; articles sans panel exclus |
| `exclusions` | liste lisible des exclusions (rétractés, paratexte, sans panel, grandes collaborations) |
| `signatures_lues` | seules les affiliations dans les pays lus comptent |
| `meme_base_que` | mêmes articles et disciplines que les indicateurs bibliométriques du périmètre (livres et chapitres en plus, hors bibliométrie) |
| `livres_chapitres` | `inclus`, `types_documents` (`book`, `book-chapter`), `domaines` (`["SH"]`), `role` (publication à part entière, jamais top 10 %), `pourquoi_shs_seulement`, `discipline`, `nb_periode`, `nb_chercheurs_actifs_avec_livres` |
| `top10` | `role` (indicateur, **pas un filtre** : le vivier compte toutes les publications), `part` (0,1), `reference`, `fenetre_citation_annees`, `non_cites_exclus`, `seuil_fraction_article`, `min_articles_top10_chercheur`, `annees_provisoires` |
| `nb_articles_perimetre`, `nb_articles_signes_pays_lus` | articles du périmètre sur la période (monde) et ceux signés dans les pays lus |

## 3. Indicateurs

| Indicateur | Nom du ratio | Numérateur (entité) | Dénominateur (`MONDE`) |
|---|---|---|---|
| Poids | `part_articles` | `nb_articles` | `nb_articles` |
| Influence | `part_top10_cites` | `nb_articles_top10_cites` | `nb_articles_top10_cites` |
| Prestige | `part_revues_tete` | `nb_articles_revues_tete_disciplinaires` (+ `…_multidisciplinaires` si l'option est cochée) | idem |
| Part d'un partenaire | `part_copublications` | `nb_articles_copublies` (entité, partenaire) | `nb_articles` de l'entité |
| ERC : taux de succès | `taux_succes` | `nb_laureats` (entité) | `nb_candidatures` (même entité) ; en porteurs : `nb_pi_laureats` / `nb_pi_candidats` |
| ERC : part des candidatures | `part_candidatures` | `nb_candidatures` (entité) | `nb_candidatures` (`MONDE`) ; en porteurs : `nb_pi_candidats` / `nb_pi_candidats` (`MONDE`) |
| ERC : part des lauréats | `part_laureats` | `nb_laureats` (entité) | `nb_laureats` (`MONDE`) ; en porteurs : `nb_pi_laureats` / `nb_pi_laureats` (`MONDE`) |
| ERC : solde de mobilité des lauréats | `solde_mobilite_laureats` | `nb_laureats_arrivees - nb_laureats_departs` (entité) | — (différence, pas un ratio) |
| ERC : intensité de candidature | `intensite_candidature` | `part_candidatures` (entité) | `part_articles` (entité) |
| Prix : part des lauréats | `part_laureats_prix` | `nb_laureats` (établissement, `prix_effectifs`) | `nb_laureats` (`FR`) |
| ERC : taux de candidature au vivier | `taux_candidature_vivier` | `nb_candidatures` (entité, `erc_effectifs`) | `nb_chercheurs` (même entité, `viviers_effectifs`) |
| Vivier : part de chercheurs top 10 % | `part_chercheurs_top10` | `nb_chercheurs_top10` (entité) | `nb_chercheurs` (même entité) |
| Prix : part de femmes | `part_femmes_prix` | `nb_laureats_femmes` (entité) | `nb_laureats_femmes + nb_laureats_hommes` (même entité) |

ERC : taux de succès = `nb_laureats` ÷ `nb_candidatures`, même entité, mêmes années, mêmes
panels et types, en additionnant les effectifs avant de diviser. Le numérateur (lauréats
signés dans l'entité) n'est **pas** un sous-ensemble du dénominateur (candidatures déposées
dans l'entité) : pour une petite entité qui accueille des lauréats venus d'ailleurs, le taux
peut dépasser 100 %. Ne pas le plafonner. Exemple : France, appels 2023, tous types :
163 lauréats signés en France / 795 candidatures déposées en France (statistiques officielles).
Solde de mobilité des lauréats = `nb_laureats_arrivees - nb_laureats_departs`, sommés sur la
même sélection ; positif : l'entité attire plus de lauréats qu'elle n'en voit partir.

ERC : mode de comptage. Les champs de chaque mode sont dans `erc_meta.modes_comptage`
(`projets` par défaut, `pi`). Taux de succès et parts se calculent avec les champs du mode
choisi, pour l'entité comme pour `MONDE` : ne jamais mélanger les deux modes dans un ratio.
Le numérateur du taux est toujours un effectif de lauréats signés dans l'entité
(`nb_laureats` ou `nb_pi_laureats`), jamais `nb_candidatures_retenues` (déposés dans
l'entité, signés où que ce soit). Les deux modes ne diffèrent que si les Synergy Grants
sont sélectionnés (un seul PI par projet pour les autres types) ; ils n'ont pas de panel et
ne comptent donc que pour « toutes disciplines ». Arrivées, départs et solde de mobilité
n'existent qu'en projets.

Intensité de candidature = `part_candidatures` ÷ `part_articles`, sur le même panel
(`discipline_code`) et la même période, pour une même entité (pays ou établissement). Les deux
parts se calculent chacune selon la règle du §4 (`erc_effectifs` pour les candidatures,
`biblio_effectifs` pour les articles), puis on divise. Au-dessous de 1, l'entité candidate
moins que son poids dans les publications ne le laisserait attendre (signe possible
d'auto-censure). Pour « toutes disciplines », restreindre les candidatures aux types avec
panel (`types_bourse_defaut`), pour comparer les mêmes champs. Les années d'appel et de
publication sont rapprochées telles quelles : choisir des périodes de même longueur.

ERC : taux de candidature au vivier = candidatures ÷ chercheurs actifs, même entité, mêmes
panels. Sert surtout à **comparer la France aux autres pays** (`erc_meta.pays_compares`, tous
présents dans `viviers_effectifs`) : candidatures du pays (pays de la structure de dépôt)
rapportées aux chercheurs actifs de ce pays.
- Années : le vivier n'a pas d'année ; prendre les appels de `viviers_meta.periode` (2021-2025
  pour le moment, couverture Horizon Europe) et diviser par le nombre d'années pour un taux annuel.
  Signaler `erc_meta.annees_provisoires`.
- Types : `types_bourse_defaut` (STG, COG, ADG), seuls à avoir un panel ; « toutes
  disciplines » = somme des panels des deux côtés.
- Disciplines : même `discipline_code` (ou mêmes panels sommés) au numérateur et au
  dénominateur ; `nb_chercheurs` (discipline principale) par défaut, ou
  `nb_chercheurs_fractionnaire`, sans les mélanger.
- Variante « excellence » : dénominateur `nb_chercheurs_top10` (chercheurs ayant au moins un
  article du top 10 %), plus proche de la population visée par l'ERC.
- Référence : l'ensemble des pays comparés (sommer candidatures et chercheurs de
  `erc_meta.pays_compares`, puis diviser). Indice = taux du pays ÷ taux de l'ensemble.
- Complément de l'intensité de candidature (`part_candidatures` ÷ `part_articles`) : ici on
  compte des personnes, pas des articles, ce qui neutralise les écarts de productivité entre pays.
Ce sont des ordres de grandeur (personnes OpenAlex, pas des effectifs RH).

Prix : mêmes prix (un groupe = la liste `prix_meta.groupes[].prix`) et mêmes années au
numérateur et au dénominateur, effectifs sommés avant de diviser. Un lauréat comptant pour
plusieurs établissements, les parts des établissements ne s'additionnent pas à 100 %.

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
  `pays_code = "FR"` ; classer par `nb_laureats` ou `taux_succes`. Prévoir un
  seuil minimal de candidatures avant de classer sur un taux (petits effectifs).
- **France et autres pays** : `$match` sur `entite_id` dans `erc_meta.pays_compares` + `MONDE`,
  `$group` par `entite_id` ; le taux de `MONDE` sert de référence. Les articles de ces pays
  sont dans `biblio_effectifs` (intensité de candidature).
- Par défaut, filtrer `type_bourse` sur `erc_meta.types_bourse_defaut`, et signaler les années
  de `erc_meta.annees_provisoires`.

```js
// Taux de succès de Sorbonne Université, panels PE, appels 2021-2025
db.erc_effectifs_<version>.aggregate([
  { $match: { entite_id: "02en5vm52", annee_appel: { $gte: 2021, $lte: 2025 },
              discipline_code: { $regex: "^PE" } } },
  { $group: { _id: null, nb_candidatures: { $sum: "$nb_candidatures" },
              nb_laureats: { $sum: "$nb_laureats" },
              nb_laureats_arrivees: { $sum: "$nb_laureats_arrivees" },
              nb_laureats_departs: { $sum: "$nb_laureats_departs" } } }
])
// taux_succes = nb_laureats / nb_candidatures (peut dépasser 1)
// en porteurs : sommer nb_pi_candidats et nb_pi_laureats ; taux = nb_pi_laureats / nb_pi_candidats
// solde_mobilite_laureats = nb_laureats_arrivees - nb_laureats_departs
```

```js
// Membres de l'IUF (juniors et seniors) d'Aix-Marseille Université, 2015-2025
const iuf = meta.groupes.find((g) => g.code === "IUF").prix;
db.prix_effectifs_<version>.aggregate([
  { $match: { entite_id: { $in: ["035xkbk20", "FR"] }, prix_id: { $in: iuf },
              annee: { $gte: 2015, $lte: 2025 } } },
  { $group: { _id: { entite_id: "$entite_id", prix_id: "$prix_id" },
              nb_laureats: { $sum: "$nb_laureats" } } }
])
// part_laureats_prix = AMU.nb_laureats / FR.nb_laureats ; liste : db.prix_laureats_<version>.find(
//   { entites: "035xkbk20", prix_id: { $in: iuf }, annee: { $gte: 2015, $lte: 2025 } })
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
| Prix | `prix.compute_prix()` | `prix/prix_effectifs.parquet`, `prix_catalogue.parquet`, `prix_laureats.parquet`, `prix_entites.parquet`, `prix_meta.json`, `prix_non_rattaches.csv`, `prix_personnes.parquet` (interne) |
| Prix : disciplines | `publications_auteurs.extraire_publications()` (lecture commune avec les viviers) puis `prix_disciplines.compute_prix_disciplines()` | `prix/publications/`, `prix_laureats_disciplines.parquet`, `prix_effectifs_disciplines.parquet`, `prix_disciplines_meta.json` |
| Viviers | `publications_auteurs.extraire_publications()` (lecture commune avec les prix) puis `viviers.compute_viviers()` | `viviers/auteurs/`, `viviers_chercheurs.parquet`, `viviers_effectifs.parquet`, `viviers_meta.json` |
| 7 | `publish.publish()` puis `publish.publish_mongo()` | `publication/erc/*.jsonl.gz` puis MongoDB |
