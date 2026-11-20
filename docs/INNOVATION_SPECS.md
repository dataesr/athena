# Spécifications — volet Innovation du tableau de bord Recherche

**Statut : proposition de cadrage, à valider avant développement.** Ce document
ne décrit pas des indicateurs déjà produits. Il s'adresse aussi bien à une personne
qu'à un agent chargé de l'implémentation.

## 1. Objet et périmètre

La demande ministérielle vise une analyse, institut par institut au sein des ONR,
du stock de chercheurs et des recrutements des cinq dernières années. Ce volet
traite uniquement de **résultats et collaborations d'organisations** : brevets,
projets collaboratifs, mentions d'entreprises privées et créations de startups.
Il ne permet ni d'identifier le stock de chercheurs, ni d'attribuer ces résultats
aux seules personnes recrutées sur cinq ans. Une somme glissante de résultats
sur cinq ans n'est **pas** le flux de recrutements demandé.

Trois vues sont visées :

| Vue            | Question                                             | Sélection                                                                                                  |
| -------------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| PROFIL         | Quel est le profil d'innovation d'un établissement ? | Établissement, année ou période, panel ERC ou toutes disciplines                                           |
| ÉTABLISSEMENTS | Comment comparer des établissements ?                | Plusieurs établissements, même année ou période, même discipline                                           |
| INTERNATIONAL  | Quelle place occupe la France parmi d'autres pays ?  | Pays participants ou déposants, même période et même discipline, seulement si la couverture est comparable |

La maille de départ est l'établissement, identifié par son ROR, **pas l'institut
d'un ONR**. Un institut n'a pas nécessairement de ROR propre et ne peut pas être
reconstitué en répartissant arbitrairement le ROR de l'organisme. Une future maille
institut nécessitera un identifiant dédié et des liens laboratoire → institut
datés, documentés et validés par les ONR. Le RNSR, Paysage et les référentiels
internes sont des pistes ; leur couverture et leur qualité restent à vérifier.

Principes : sources réutilisables en priorité, lots indépendants, reproductibilité,
publication des limites et aucun résultat manquant présenté comme zéro. La
présence d'une source dans scanR ne garantit ni son exhaustivité, ni le droit de
redistribution de ses données détaillées ou agrégées.

## 2. Ce que fait déjà Athena

Les éléments ci-dessous sont les points d'appui réels du dépôt :

- [extract_works.py](../project/server/main/extract_works.py) extrait les works
  OpenAlex en Parquet : identifiant, année, **topic principal**, pays et liens
  work–institution. Les works sans topic principal sont exclus de cette extraction.
  Elle ne conserve pas le DOI : un appariement scanR → OpenAlex par DOI nécessite
  donc une extraction complémentaire ou une modification maîtrisée de l'extraction.
- [disciplines.py](../project/server/main/disciplines.py) charge la table
  [topics OpenAlex → panels ERC](../topics/openalex_topics_erc_panels.json) :
  **un topic → un panel**, avec une alternative pour les cas douteux. La table
  existe déjà ; les publications utilisent leur topic principal, pas une
  affectation multi-panels.
- [indicators.py](../project/server/main/indicators.py) compte en **entier** un
  article une fois par pays et par établissement présent, y compris les tutelles
  via la lignée OpenAlex corrigée. `MONDE` compte chaque article une fois.
  Le fractionnement du top 10 % concerne le traitement des ex aequo, pas
  l'attribution institutionnelle.
- [horizon.py](../project/server/main/horizon.py) lit les participations Horizon
  Europe des jeux ESR, normalise les identifiants et rattache structures et
  tutelles RNSR aux ROR actuels via Paysage. [erc.py](../project/server/main/erc.py)
  compte des projets ERC distincts en entier par entité et utilise l'**année
  d'appel**, le panel natif et la structure au dépôt ou à la signature selon le
  jeu. Le jeu des candidatures est privé ; les lauréats sont le point de départ
  envisagé ici, sous réserve de confirmer accès et droits.
- [extract_awards.py](../project/server/main/extract_awards.py) calcule des
  financements OpenAlex par **pays du financeur**, année de début et topic
  principal. Ce n'est pas un calcul de projets Pilier 2 par établissement
  bénéficiaire. [projects.py](../project/server/main/projects.py) charge des
  projets scanR dans MongoDB ; ce n'est pas le pipeline d'indicateurs décrit ici.
- [publish.py](../project/server/main/publish.py), [pipeline.py](../project/server/main/pipeline.py)
  et [tasks.py](../project/server/main/tasks.py) organisent calculs et publication.
  Le [contrat de données](../CONTRAT_DONNEES.md) prescrit des noms français,
  des effectifs stockés et des ratios calculés à la lecture.

## 3. Conventions proposées

### 3.1 Grain et rattachements

Le triplet souhaité est `entite_id × annee × discipline_code`, complété par
`entite_type`, l'indicateur, son périmètre et sa version. Il n'est pas toujours
observable : les startups, brevets et projets sans discipline fiablement classée
restent disponibles **toutes disciplines**, sans être inventés dans un panel.

`entite_type` : `etablissement` (ROR court), `pays` (ISO2, France `FR`) ou
`monde` (`MONDE`). Il faut relier les établissements à la collection commune
`entites`, y compris ceux présents uniquement dans l'innovation ; vérifier leur
présence dans le sélecteur. Les liens indirects de tutelle sont conservés avec
leur origine. Successeurs Paysage et lignée OpenAlex décrivent une lecture avec
**structures actuelles**, pas nécessairement les tutelles à la date du résultat.
Ne pas assimiler un lien indirect à la propriété juridique d'un brevet ou à un
bénéficiaire effectif de financement.

Par défaut, pour rester comparable aux autres volets : **comptage entier**,
objet distinct une fois par entité, année et panel. Un objet partagé compte
pour chacune de ses entités ; les sommes entre établissements ou pays peuvent
donc dépasser le total France ou monde. Le total France et `MONDE` sont
calculés directement sur les objets distincts, jamais par somme des ROR. Un
mode fractionnaire serait une option ultérieure, distincte, après définition du
périmètre des participants, des tutelles et des identifiants non résolus ; il
ne faut pas le mélanger au mode entier.

La période glissante de cinq années civiles se calcule pour les flux par
somme des effectifs annuels **si chaque objet n'a qu'une année de référence**.
Les stocks et les nombres d'entreprises distinctes sur plusieurs années ne sont
pas additifs : recalculer les distincts sur la période ou ne pas proposer ce
cumul. Les montants ne sont additionnables qu'à devise et définition identiques.

### 3.2 Discipline

Les 28 panels (`PE1`–`PE11`, `LS1`–`LS9`, `SH1`–`SH8`) constituent le
référentiel cible. `discipline_code = null` signifie « non classé » ; la
sélection toutes disciplines inclut les lignes classées **et** non classées.
Ne pas stocker en plus une ligne `ALL` dans la même table, pour éviter de la
compter deux fois. Une sélection par panel exclut les non-classés et indique
le taux de classement ; si aucun classement valide n'existe pour une source,
l'indicateur est marqué **indisponible pour ce panel**, pas zéro.

| Objet           | Approche initiale                                                                   | Statut                                                                                |
| --------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| Publication     | Topic principal du work OpenAlex apparié, puis table Athena existante               | Disponible après appariement et contrôle du périmètre                                 |
| Brevet          | CPC natif pour la navigation technologique ; correspondance CPC → ERC expérimentale | À construire et valider : CPC est une classification technologique, pas disciplinaire |
| Projet Pilier 2 | Aucun panel ERC présumé ; texte et nomenclature du programme comme pistes           | À construire et valider ; un cluster n'est pas un panel ERC                           |
| Startup         | Aucune affectation initiale                                                         | Non classée ; liens PI → brevet ou secteur BPI à explorer, non probants par défaut    |

Pour les brevets et projets, comparer une table explicite plusieurs-à-plusieurs
CPC/discipline ou un classifieur fondé sur titre et résumé, éventuellement
informé par la bibliométrie liée. Aucune méthode n'est imposée avant étude de
faisabilité. Pour préserver l'additivité des panels, retenir au maximum **un
panel principal** par objet en version publiée, avec possibilité d'abstention.
Les autres hypothèses et scores restent en données de contrôle. Versionner
mapping, modèle, seuil, vocabulaire et échantillon de validation ; mesurer
précision, rappel et couverture par panel, année et source, pas seulement la
performance globale. Une éventuelle répartition pondérée multi-panels est un
mode séparé à spécifier ultérieurement.

### 3.3 Parts et états

Une part est un ratio calculé après agrégation des numérateurs et dénominateurs
sur le même périmètre, année, discipline, méthode et version. Si le dénominateur
est nul ou indisponible : ratio `null`, jamais zéro ni infini. La part d'un
établissement dans la France peut être calculée si ses objets sont un sous-ensemble
du périmètre France ; la somme de ces parts peut dépasser 100 % à cause des
tutelles et copropriétés.

Pour chaque indicateur et sélection, distinguer : `disponible` (zéro observable
autorisé), `provisoire` (délai ou couverture incomplète), `indisponible`
(source ou combinaison non couverte), et `non_classe` pour les objets sans
panel. Documenter les années effectivement couvertes, la date d'observation,
les exclusions, les taux de rattachement et la méthode. Aucun seuil d'affichage
automatique n'est retenu sans validation métier.

## 4. Sources et faisabilité

Constats issus de la lecture du dépôt, des deux pièces jointes et d'une exploration
des schémas scanR et du jeu ESR. Il s'agit de constats ponctuels, non d'un audit
statistique ou juridique.

| Source                                                                                                                       | Ce qui est observable                                                                                                                                                          | Conditions et limites                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scanr-patents`                                                                                                              | `inpadocFamily`, `applicants.ids`, pays, `cpc`, documents `patents` avec dates de dépôt et de publication                                                                      | Un exemple comporte plusieurs documents pour une famille. Origine, périmètre géographique, droit d'usage et signification de `year` restent à confirmer. Pas de champ explicite de date de priorité identifié dans le schéma consulté.                                                 |
| `scanr-projects` / `scanr-participations`                                                                                    | Identifiant projet, type, `startDate`, participants, `participation_funding` et budget de projet                                                                               | `budgetFinanced` est au niveau projet ; ne pas le traiter comme montant reçu par chaque participant. Pilier 2, types d'entreprise, devises et droits à vérifier.                                                                                                                       |
| [Horizon Europe — entités](https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-horizon-projects-entities/) | Source déjà utilisée par `horizon.py`, une ligne de participation ; `pilier_code`, `call_year`, identifiants et pays ; le champ source `fund_eur` est la contribution en euros | Accès API observé sans authentification, mais métadonnée `visibility=domain` : confirmer la pérennité d'accès et les droits de redistribution. `horizon.py` ne garde pas encore `fund_eur` dans ses données normalisées. Horizon Europe uniquement, pas de rétropolation Horizon 2020. |
| `scanr-publications`                                                                                                         | `structured_acknowledgments.private_companies`, noms, mentions, rôles ; identifiants externes dont DOI                                                                         | Un exemple observé contient un rôle `funding`, ce qui ne prouve pas que toutes les mentions sont des financements. Couverture, méthode d'extraction, taux d'erreur, droits et identifiants OpenAlex à vérifier.                                                                        |
| [Startups ESR](https://data.enseignementsup-recherche.gouv.fr/explore/assets/fr-esr-startup-esr-france/)                     | SIREN, date de création, structures liées, actifs, accompagnement, statut et observation                                                                                       | Licence Ouverte v2.0 selon la pièce jointe. Recensement ONR depuis 2012, complété par IRP et SATT ; couverture variable et délai de repérage.                                                                                                                                          |
| `fr-bpi-data-perimetres-startups`                                                                                            | Selon la demande : SIREN, date d'entrée, secteurs, indicateurs deeptech/greentech/industrie                                                                                    | Jeu privé, disponibilité et droits inconnus ; le traitement public doit fonctionner sans lui. Une date d'entrée dans le périmètre n'est pas une date de création.                                                                                                                      |
| Labcom, Carnot, Cifre, CIR                                                                                                   | Besoins recensés par la note DGRI                                                                                                                                              | Sources, champs, rattachements et droits à obtenir ; pas d'indicateur publié par défaut.                                                                                                                                                                                               |

### 4.1 Résultats de la qualification initiale (18 novembre 2026)

Les chiffres ci-dessous proviennent d'une lecture complète de l'API du jeu
startups et d'agrégations de l'API du jeu Horizon à cette date. Ils décrivent
le millésime consulté, pas une garantie de couverture historique ni de droits.

**Startups ESR**

- Le jeu renvoie 4 680 enregistrements. 10 n'ont pas de SIREN ; parmi les
  4 670 SIREN renseignés, aucun doublon n'a été observé.
- 21 dates de création et 22 statuts sont absents. 1 915 lignes (40,9 %) n'ont
  ni `structures_liees_id` ni `structures_liees`; les autres 2 765 ont un lien.
- Les liens comptent 15 245 identifiants et 200 valeurs distinctes, toutes de
  14 chiffres (forme de SIRET). Deux lignes ont un nombre d'identifiants
  différent du nombre de noms dans `structures_liees` : ne pas joindre ces
  deux champs par position.
- Une recherche exacte de 109 SIRET distincts (présents dans les 100 premières
  lignes du jeu) les retrouve tous dans `scanr_organizations`. Elle retourne
  114 documents d'organisation, dont 108 (94,7 %) portent un identifiant ROR.
  Certains SIRET apparaissent sur plusieurs documents : ces pourcentages sont
  des comptes de documents, pas une couverture par SIRET ni une validation de
  correspondance unique. Résoudre les 200 SIRET, mesurer pour chacun le nombre
  de documents et de ROR distincts, et mettre les ambiguïtés en contrôle avant
  de calculer.
- `observation_etat` est renseigné dans toutes les lignes et vaut
  `07-07-2026` dans le millésime consulté. Le champ annoncé
  `incubateur_public_id` est absent ; `incubateur_public_lib` est renseigné
  dans 2 559 lignes. STU-04 ne peut donc pas s'appuyer sur un identifiant
  stable d'incubateur dans ce millésime.
- Les années de création vont de 1974 à 2026 ; les métadonnées jointes
  signalent un repérage plus fiable depuis 2012 et des retards de complétude.
  Exclure les dates manquantes des séries annuelles, afficher leur nombre et
  traiter 2026 comme année incomplète.

**Horizon Europe**

- L'API a répondu sans clé lors du test. Le jeu contient 146 887 participations ;
  `pilier_code=HORIZON.2` en représente 86 942, pour 5 270 `project_id`
  distincts dans l'agrégation testée. Les accès et la réconciliation des
  projets entre millésimes restent à documenter.
- Au Pilier 2, 56 326 participations déclarent une source d'identifiant
  `paysage`, 311 `ror`, 106 `siren`, 1 `siret` et 30 198 n'ont pas de
  `entities_id_source`. Les exemples sans source contiennent des identifiants
  numériques qui semblent être des SIREN, mais ne doivent pas être classés
  ainsi sans contrôle. Le rattachement existant par Paysage couvre donc une
  partie des lignes, pas la totalité.
- `panel_code` est null dans les 86 942 lignes Pilier 2 consultées ; les
  exemples hors Pilier 2 peuvent contenir des codes tels que `ENG` ou `PHY`,
  qui ne sont pas les 28 panels ERC. Aucun de ces codes ne doit être publié
  comme panel ERC sans table de correspondance validée.
- `fund_eur` est décrit comme financement obtenu en euros au niveau de la
  participation. Avant PRJ-02, vérifier son traitement pour les partenaires
  associés et la règle de rattachement aux tutelles ; la fonction
  `horizon.participations()` ne le transporte pas encore.
- Le Pilier 2 comporte 6 projets / 92 participations d'appel 2026, tous au
  statut `UNDER_PREPARATION` dans les données consultées ; 2025 contient aussi
  7 462 participations à ce statut. Le filtre des projets financés doit donc
  traiter `status_code` explicitement et valider avec le producteur quels
  statuts (signés, clos, suspendus, terminés) correspondent à un financement
  effectivement obtenu. Exclure les lignes en préparation du nombre de projets
  financés ; afficher les années récentes comme provisoires.

#### Qualification complémentaire Horizon (20 novembre 2026)

Les résultats suivants proviennent de requêtes d'agrégation de l'[API ODS du
jeu des lauréats](https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-horizon-projects-entities/records)
avec `pilier_code="HORIZON.2"`. Ils décrivent le jeu consulté, pas une
garantie de couverture ni les règles sémantiques du producteur.

| `status_code`       | Participations | Projets distincts indiqués par l'API |
| ------------------- | -------------: | -----------------------------------: |
| `SIGNED`            |         75 913 |                                4 563 |
| `CLOSED`            |          2 932 |                                  203 |
| `SUSPENDED`         |            285 |                                   21 |
| `TERMINATED`        |            109 |                                    9 |
| `UNDER_PREPARATION` |          7 703 |                                  461 |

Toutes ces lignes ont `stage="successful"` : ce champ ne permet donc pas à lui
seul de distinguer les projets en préparation. La description du champ
`status_code` ne définit pas les codes ; notamment, ne pas déduire d'un statut
terminé, clos ou suspendu que le financement a effectivement été perçu. La
sélection des statuts pour PRJ-01 reste à valider avec le producteur.
Pour le pilote, le périmètre retenu est `SIGNED` uniquement, avec un libellé
« projets à convention signée » et un statut provisoire ; tous les types de
participants sont comptés. Ce choix n'établit pas le versement d'un financement.
La lecture complète de l'export CSV public consulté le 20 novembre ne révèle
aucun projet Pilier 2 avec plusieurs statuts ou plusieurs années d'appel non
vides ; ce contrôle sera répété par le calcul sur chaque nouvelle extraction.

Les lignes `SIGNED` comprennent 65 804 participations `beneficiary`, 6 086
`associated partner` et 4 023 `thirdparty`. Le choix des rôles à attribuer
aux établissements doit être explicite : participer à un projet ne signifie
pas nécessairement en être bénéficiaire financier. Les nombres de projets par
groupe ne doivent pas être additionnés.

Pour les participations françaises (`country_code="FRA"`), tous statuts
confondus, 7 061 lignes indiquent un identifiant Paysage, 105 un SIREN, une un
SIRET et 50 ne précisent pas la source de l'identifiant. Au total Pilier 2,
30 198 lignes n'ont pas de source d'identifiant ; la couverture du rattachement
effectif reste à mesurer avec les référentiels, sans deviner le type de ces
identifiants.
Dans l'export complet consulté, le périmètre provisoire `SIGNED` comprend
6 285 lignes de participation françaises (outre-mer inclus), pour 2 514
projets distincts avec une année d'appel valide ; 29 de ces lignes comportent
un identifiant numérique à neuf chiffres sans source. Ces chiffres précèdent
le rattachement aux établissements et évolueront avec les données.

Les [métadonnées ODS](https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-horizon-projects-entities)
indiquent la Licence Ouverte v2.0 et `visibility="domain"` ; l'API répondait
sans clé lors de la consultation, sans garantie de pérennité de cet accès.
La date `modified` indiquée est le 19 septembre 2026, mais les métadonnées
précisent qu'elle n'est pas actualisée automatiquement lors des changements
de données ou de métadonnées : ne pas l'utiliser comme date certaine du
millésime des enregistrements.

Pour les startups, la pièce jointe décrit `structures_liees` comme champ
d'analyse, séparateur `|`, et `structures_liees_id` comme identifiants sans
en préciser la nature. Un enregistrement public consulté présente un identifiant
à **14 chiffres** dans ce dernier champ : ne pas supposer qu'il s'agit d'un ROR,
ni que tous les identifiants sont du même type. Valider leur typologie et le
rapprochement, potentiellement via SIRET/SIREN et un référentiel d'organisations ;
les noms seuls servent à proposer une correspondance à relire, jamais à une
attribution silencieuse. Vérifier également l'alignement noms/identifiants : deux
listes ne sont pas forcément positionnellement correspondantes.

La pièce jointe nomme `date_observation_statut` et `incubateur_public_id` ; dans
les métadonnées publiques consultées, le champ d'observation est
`observation_etat` et `incubateur_public_id` n'apparaît pas. `incubateur_public_lib`
est présent. **Inspecter les colonnes du millésime effectivement téléchargé**,
documenter les alias et échouer explicitement si une colonne indispensable manque.

Vérification complémentaire de l'API ODS v2.1 : les noms observés sont
`siren_startup`, `date_creation_startup`, `structures_liees_id` et
`observation_etat`. L'API renvoie les identifiants liés comme une liste ; le
téléchargement CSV peut les représenter en texte. Le pilote accepte les listes
et leurs séparateurs documentés, valide chaque SIRET sur 14 chiffres et échoue
si une colonne indispensable manque. Il ne joint jamais `structures_liees`
par position à `structures_liees_id`.

## 5. Catalogue des indicateurs proposés

Les identifiants sont stables à titre de proposition. « Conditionnel » signifie
qu'une preuve de couverture, de droits et de qualité est indispensable avant
publication. L'année et le périmètre sont propres à chaque indicateur : une
année de dépôt de brevet n'est pas une année d'appel Horizon.

### 5.1 Brevets

| ID         | Indicateur et calcul                                                                                                                                                                                     | Statut                                                                     |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| INN-BRE-01 | Nombre de familles INPADOC distinctes avec au moins un déposant rattaché à l'entité ; année de priorité **si elle est vérifiable**, sinon année de dépôt selon une règle validée et libellée comme telle | Conditionnel                                                               |
| INN-BRE-02 | Part France : BRE-01 établissement / familles distinctes France, même périmètre, année et discipline                                                                                                     | Conditionnel ; ne pas utiliser la somme des établissements                 |
| INN-BRE-03 | Familles avec au moins un déposant public et un déposant entreprise distincts                                                                                                                            | Exploration : qualification juridique et identifiants à valider            |
| INN-BRE-04 | Familles par code CPC : famille distincte par code ; une famille multiclassée compte dans plusieurs codes                                                                                                | Conditionnel ; les catégories CPC ne s'additionnent pas sans dédoublonnage |

Une famille peut avoir plusieurs documents, dates et déposants. Dédoublonner sur
`inpadocFamily`, conserver l'identifiant et la provenance ; si absent, isoler
les cas sans famille au lieu de créer une fausse famille commune ou de les
compter comme familles distinctes sans preuve. Vérifier si plusieurs documents
scanR peuvent représenter la même famille. Une date issue de
`patents.applicationDate` avec `isPriority` n'est pas automatiquement la vraie
date de priorité : valider sa sémantique et les éventuelles priorités multiples.
Le délai de publication, souvent proche de 18 mois, rend les années récentes
incomplètes : afficher un avertissement et un état provisoire, sans promettre
une date universelle de complétude. La publication ne démontre ni délivrance,
ni valeur économique.

### 5.2 Projets européens et recherche partenariale

| ID               | Indicateur et calcul                                                                                                              | Statut                                                                                                              |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| INN-PRJ-01       | Projets Horizon Europe Pilier 2 financés : `project_id` distincts par entité ; année d'appel pour cohérence avec ERC, si présente | Candidat après validation de `status_code`, des identifiants et de la licence                                       |
| INN-PRJ-02       | Contribution UE aux participants effectivement bénéficiaires, somme de `fund_eur` par entité et année d'appel, en euros           | Champ identifié dans la source ; valider traitement des associés, identifiants, droits et rattachement aux tutelles |
| INN-PRJ-03       | Projets Pilier 2 comportant au moins un partenaire entreprise distinct                                                            | Exploration : type et périmètre des partenaires à valider                                                           |
| INN-PRJ-04/05/06 | Labcom, Carnot, Cifre                                                                                                             | À obtenir et définir ; pas de zéro par défaut                                                                       |

Source prioritaire proposée : le jeu Horizon des **lauréats** déjà lu par
`horizon.py`, avec distinction projet/participation. Ne pas filtrer les seuls
PI ERC : on recherche ici les participants aux projets du Pilier 2. Si le
jeu n'a pas le champ requis, étudier scanR comme source complémentaire, avec
appariement sur `project_id` et contrôle des périmètres, pas addition des deux.
Pour PRJ-02, dédoublonner les participations avant somme ; ne pas copier le
budget total du projet sur chaque partenaire. Le montant légalement reçu par un
bénéficiaire et une attribution analytique à ses tutelles sont deux mesures
différentes : publier la première seulement ; la seconde nécessite une règle
approuvée, sans addition trompeuse. Les projets ERC restent dans le volet
reconnaissance, pas dans le Pilier 2 par simple présence dans Horizon.

### 5.3 Mentions d'entreprises dans les publications

| ID         | Indicateur et calcul                                                                                                            | Statut                                                                                           |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| INN-ACK-01 | Publications distinctes avec au moins une entreprise privée détectée dans les remerciements, par année de publication et entité | Exploration                                                                                      |
| INN-ACK-02 | ACK-01 / publications du **même univers observable**, par entité, année et panel                                                | Exploration ; seulement si l'observabilité est mesurable                                         |
| INN-ACK-03 | Entreprises distinctes mentionnées sur la sélection                                                                             | Ultérieur ; nécessite des identifiants désambiguïsés et un recalcul des distincts sur la période |

Une mention n'est pas nécessairement un financement, un partenariat direct
avec l'établissement, ni une innovation. Par défaut : libellé « publications
mentionnant une entreprise privée dans les remerciements ». Un indicateur
« financement » distinct exige une règle validée sur `role` et un échantillon
annoté. Ne jamais supposer que l'absence de mention signifie absence de
financement : distinguer texte absent, extraction indisponible, extraction sans
entreprise et résultat positif. Une publication scanR sans correspondance
OpenAlex n'est pas un zéro ; la conserver dans les contrôles.

Appariement : identifiant OpenAlex explicite s'il est vérifié, sinon DOI
normalisé, unique et non ambigu dans les deux sources ; exclure les collisions
et les reporter. Retenir la même population de publications appariées pour le
numérateur et le dénominateur, avec mêmes filtres de type, année, revue, topic
et affiliation. La simple division par tous les articles OpenAlex biaiserait
la part lorsque scanR n'observe qu'une fraction des remerciements. Si le
traitement des remerciements ne peut être distingué de l'absence de données,
ne pas publier ACK-02 : publier au mieux un nombre détecté, accompagné du taux
de couverture et du biais possible.

### 5.4 Startups

| ID         | Indicateur et calcul                                                                                                            | Statut                                                                                                                                              |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| INN-STU-01 | SIREN distincts créés dans l'année, liés à l'établissement ; date de création du SIREN                                          | Premier candidat après résolution des structures                                                                                                    |
| INN-STU-02 | SIREN distincts actifs **à la date d'observation du millésime**                                                                 | Conditionnel ; pas un stock historique par année                                                                                                    |
| INN-STU-03 | Survie à 3 ou 5 ans d'une cohorte de création                                                                                   | Ultérieur : historique fiable des fermetures, dates et censure à établir                                                                            |
| INN-STU-04 | Startups créées dans l'année et liées à l'établissement, avec au moins un incubateur public renseigné (`incubateur_public_lib`) | Conditionnel ; millésime consulté sans identifiant d'incubateur stable, indique l'accompagnement mais pas nécessairement durant l'année de création |
| INN-STU-05 | Startups deeptech / greentech / industrielles                                                                                   | Ultérieur, uniquement si droits BPI et jointure par SIREN validés                                                                                   |

`structures_liees` décrit un lien ESR, pas forcément une création attribuable
exclusivement à l'établissement. Une startup multi-liée compte une fois pour
chaque ROR résolu. SIREN est une chaîne de neuf chiffres, zéros initiaux inclus ;
signaler les identifiants absents ou invalides et ne pas les dédoublonner
arbitrairement. Dates de création manquantes : conserver en contrôle, hors
séries annuelles ; statut inconnu : hors numérateur **et** dénominateur d'un
éventuel taux de survie, avec nombre d'exclusions. Ne pas déduire qu'une
startup active à la date du millésime l'était à toutes les dates antérieures.
Pour la survie, fixer l'âge, la date d'observation, la gestion des fermetures
partielles et des observations censurées ; une cohorte trop récente n'est pas
éligible. Afficher les biais de repérage, la date du millésime et les années
récentes provisoires.

## 6. Comparaison internationale

Ne pas activer automatiquement la vue par axe : une source « internationale »
peut n'être qu'un sous-ensemble sélectionné sur la France. Mesurer la couverture
par pays, année, discipline et type d'objet ; vérifier les licences et comparer
avec une référence indépendante avant publication.

| Axe           | Décision proposée                                                                                                                                                                                                                                          |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Brevets       | À étudier : évaluer la sélection de `scanr-patents`. Si elle est centrée sur la France, explorer une source mondiale telle que PATSTAT, sous réserve de droits, coûts, accès et méthode homogène. Ne pas promettre une comparaison partageable à ce stade. |
| Pilier 2      | Candidat si le jeu des lauréats couvre l'ensemble des participants étrangers et si les pays sont fiables ; pays du participant, pas pays du financeur. Pays et `MONDE` dédoublonnés par projet ; un projet multinational compte dans plusieurs pays.       |
| Remerciements | Non par défaut : il faut une extraction mondiale comparable et un univers observable par pays.                                                                                                                                                             |
| Startups      | Non : jeu ESR centré sur la France, pas de dénominateur international comparable.                                                                                                                                                                          |

Un panel non classé ou une année provisoire restent signalés dans chaque pays.
Ne pas interpréter le nombre de projets européens comme l'ensemble de
l'innovation d'un pays, ni comparer directement des indicateurs de sources
ayant des populations différentes.

## 7. Lots et critères de passage

| Lot                 | Livrable                                                                                                                   | Porte de passage                                                                                                                                                                               |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0 — qualification   | Schémas et premiers profils des sources startups et Horizon ; points à résoudre documentés en §4.1                         | Compléter les mesures de rattachement startups et statuts/identifiants Horizon ; confirmer droits de redistribution                                                                            |
| 1 — pilote startups | Un graphique d'évolution STU-01 toutes disciplines ; STU-04 hors périmètre                                                 | Appliquer la règle commune d'affiliation, contrôler les SIRET non résolus et afficher les limites de couverture ; les lignes sans lien comptent dans le total France, jamais dans un établissement |
| 2 — pilote Horizon  | PRJ-01 Pilier 2 ; PRJ-02 si `fund_eur` et le traitement des participations sont validés                                    | Définir les statuts financés, qualifier les identifiants (notamment source absente), intégrer `fund_eur` dans la normalisation et ne pas affecter de panels ERC par défaut                     |
| 3 — enrichissements | ACK-01 et éventuellement ACK-02 ; classification ERC des brevets/projets si validée ; INTERNATIONAL Pilier 2 si comparable | Validation métier de chaque mesure et de la classification ; couverture internationale démontrée                                                                                               |
| 4 — extensions      | Copropriété, entreprises distinctes, startups BPI, survie, Labcom/Carnot/Cifre et éventuellement brevets internationaux    | Sources et droits obtenus, définitions et contrôles validés                                                                                                                                    |

Les lots sont indépendants par source : l'échec d'une source optionnelle ne doit
pas empêcher la production des autres, mais doit apparaître dans les métadonnées
et dans l'interface. Une source explicitement demandée et absente fait échouer
son étape ; pas de succès factice. Les seuils de qualité et le niveau de risque
acceptable sont à arrêter avec les utilisateurs après les mesures du lot 0.

## 8. Proposition d'implémentation technique

### 8.1 Chaîne de traitement

```text
sources versionnées → copies brutes → objets et liens normalisés
                    → résolution identifiants / tutelles → classification éventuelle
                    → contrôles et quarantaines → effectifs + métadonnées
                    → publication Athena → API / vues tableaux
```

Réutiliser DuckDB/Parquet, `utils_ods.get_ods_data`, les référentiels de
`referentiel.py`, `horizon.py` et la table `disciplines/erc.parquet`. Préférer
les dumps ou exports officiels documentés pour un traitement reproductible :
le MCP scanR sert ici à l'exploration, pas de contrat d'ingestion de production.
La pagination Elasticsearch, si retenue, doit être complète et stable, avec
snapshot/version, reprise, contrôles de volume et erreurs explicites ; ne pas
prendre les premiers résultats d'une recherche pour le jeu complet.

### 8.2 Fonctions principales à créer

Noms indicatifs, à ajuster aux interfaces du dépôt ; pas d'obligation de créer
tous les modules dès le premier lot.

| Module proposé                | Fonction et responsabilité                                                                                                                  | Entrées / sorties principales                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `startups.py`                 | `lire_startups()` : télécharger ou relire une copie datée, vérifier le schéma et normaliser SIREN/dates/listes                              | Export ESR → Parquet brut normalisé ; alias de colonnes documentés                                  |
| idem                          | Résoudre les structures avec la règle commune et contrôler les non-rattachés                                                               | Startups + référentiels → ROR de rattachement et non-rattachés                                       |
| idem                          | `compute_startups()` : calculer STU-01/04 et contrôles ; STU-02 à date d'observation seulement                                              | Liens dédoublonnés → effectifs et métadonnées                                                       |
| `innovation_horizon.py`       | `compute_projets_horizon()` : exploiter `lire_horizon`, `participations`, `rattacher_structures`, filtrer lauréats Pilier 2 et dédoublonner | Données Horizon + référentiels → projets, liens et PRJ-01 ; éventuel PRJ-02 séparé                  |
| `innovation_brevets.py`       | `lire_brevets()`, `normaliser_familles()`, `rattacher_deposants()`, `compute_brevets()`                                                     | Export qualifié → familles, documents, déposants, liens et BRE-01/04 ; dates manquantes en contrôle |
| `innovation_remerciements.py` | `apparier_publications()`, `qualifier_remerciements()`, `compute_remerciements()`                                                           | scanR + identifiants OpenAlex + works → appariements, états observables, ACK et couverture          |
| `innovation_disciplines.py`   | `classer_objets()` et `controler_classement()` **uniquement après validation de méthode**                                                   | Objets + mapping/modèle versionné → un panel ou null, score, diagnostics                            |
| `innovation.py`               | `compute_innovation()` : orchestrer les axes sélectionnés, contrôler et produire les fichiers sans masquer les erreurs                      | Paramètres + sorties d'axes → effectifs, catalogue et métadonnées                                   |

Les fonctions d'ingestion conservent les identifiants source, dates, version,
champs utiles, statut et origine des liens ; les données BPI restent séparées.
Pour les autres rapprochements à concevoir, conserver une table de correspondance
versionnée avec preuve et statut (`exact`, `valide_manuellement`, `ambigu`, `absent`)
et ne pas transformer une correspondance ambiguë en ROR automatiquement.
Pour les publications, une extraction complémentaire DOI → `work_id` doit
couvrir le même snapshot et les mêmes filtres que les works utilisés ; mesurer
les collisions et les works absents. Prévoir un fichier de contrôle des
non-rattachés pour chaque axe, sans publier de données non partageables.

### 8.3 Contrat de sortie proposé

À valider avant modification du [contrat existant](../CONTRAT_DONNEES.md).
Exemples de fichiers dans `<DATA_DIR>/innovation/` :

| Fichier                                              | Grain et contenu                                                                                                                                                                                                                                                            |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `innovation_effectifs.parquet`                       | `indicateur_id`, `perimetre`, `classement`, `entite_type`, `entite_id`, `annee`, `discipline_code` nullable ; `valeur` numérique, `unite` (`nombre` ou `EUR`), `statut`. Une seule ligne par clé et version. `annee` est l'année de référence documentée dans le catalogue. |
| `innovation_catalogue.parquet`                       | Définitions, année de référence, unité, niveau géographique, additivité, disponibilité par panel et dénominateur associé pour chaque indicateur.                                                                                                                            |
| `innovation_meta.json`                               | Sources et droits, dates/millésimes et empreintes, paramètres, méthodes, version des mappings et référentiels, couverture, exclusions, années provisoires, textes à afficher et états par axe.                                                                              |
| `innovation_non_rattaches.csv` et contrôles internes | Objets et liens non résolus, raison et origine ; diffusion selon droits.                                                                                                                                                                                                    |

`valeur` représente des **effectifs ou montants**, pas des ratios. Les
dénominateurs d'ACK et autres parts sont des effectifs distincts identifiés
dans le catalogue et calculés à la lecture ; les ratios ne sont pas stockés.
Une mesure de stock à date d'observation n'est pas une série annuelle : sa
date exacte doit être portée dans la clé ou le périmètre et l'interface ne
doit pas additionner ces stocks. Pour les CPC et entreprises distinctes, le
grain doit intégrer la dimension concernée ou utiliser une table séparée ;
ne pas additionner ces ventilations comme si elles étaient exclusives.

`null` en discipline est réservé aux objets non classés, pas à une ligne
pré-agrégée « toutes disciplines ». Les résultats absents et indisponibles
sont exprimés dans les métadonnées, pas par l'absence d'une ligne interprétée
comme zéro. Une ligne à zéro ne peut être générée que si source, période,
entité et panel sont effectivement observables. `MONDE` n'est produit que
si le périmètre mondial est justifié, pas pour un jeu franco-français.

### 8.4 Intégration et affichage

1. Ajouter des paramètres explicites dans `config.py` (sources, dossiers,
   versions, options) ; aucun secret dans les fichiers ou métadonnées publiées.
2. Ajouter les étapes sélectionnables dans `pipeline.py` et `tasks.py`, avec
   dépendances explicites et ordre avant `publish`. Une étape optionnelle ne
   doit pas devenir implicitement obligatoire pour les calculs existants.
3. Étendre `publish.py` : collections d'effectifs, métadonnées et liste de ROR
   propres à l'innovation, avec index, fichiers versionnés et manifeste. Le
   pilote maintient sa propre liste de sélection pour ne pas élargir les
   sélecteurs des autres onglets. Vérifier la compatibilité des versions Mongo
   et de leur activation existante.
4. Adapter le contrat et le consommateur **tableaux** dans son propre dépôt :
   API, filtres et vues. Ne pas supposer que l'aperçu HTML Athena constitue
   le dashboard final. N'exposer publiquement ni lignes ni agrégats BPI sans
   autorisation explicite ; séparer stockage, accès et publication privée.
5. Pour chaque carte ou tableau : libellé de mesure, unité, année de référence,
   source/millésime, comptage entier, état, discipline et couverture ; donner
   accès à la méthode. Périodes, panels et pays indisponibles doivent être
   désactivés ou marqués indisponibles, jamais remplis par zéro.

### 8.5 Pilote implémenté : startups ESR

Le pilote ajoute **Innovation comme nouvel onglet de premier niveau**, au même
niveau que Bibliométrie et Financements, sans sous-onglets ni autres graphiques.
L'unique graphique est l'évolution annuelle de `INN-STU-01` : SIREN distincts
créés depuis 2012, filtrables par année et par établissement ou France. Aucun
panel ERC n'est présenté. Une startup compte une fois par ROR auquel elle est
rattachée, y compris les tutelles OpenAlex disponibles ; les effectifs par
établissement ne sont donc pas additifs. France compte une fois chaque SIREN
valide, y compris les startups sans lien exploitable.

Le pipeline facultatif `startups` télécharge
`fr-esr-startup-esr-france` via `utils_ods`, applique aux SIRET la règle commune
Paysage (repli SIREN et successeurs), puis les tutelles scanR et la lignée
OpenAlex lorsqu'elles sont disponibles. Si les fichiers de lignée manquent,
les tutelles OpenAlex ne sont pas ajoutées et les métadonnées le signalent.
Il garde localement les lignes brutes et les contrôles, puis
produit des agrégats, une liste de ROR résolus et des métadonnées. Les
collections publiables sont `innovation_effectifs`, `innovation_entites` et
`innovation_meta`. L'API en lecture seule expose
`/suivi-recherche/innovation/startups/meta`, `/entites` et `/evolution` avec
`{ data, provenance }`; le client utilise le même sélecteur d'établissement et
de période que les autres onglets, mais masque le filtre de discipline.

L'étape ne fait pas partie du calcul par défaut. `publish()` écrit les fichiers ;
aucune exécution de `publish_mongo()`, activation de collections, inscription
ou déploiement n'est faite par ce pilote. L'exécution sur le jeu complet et la
validation humaine du tableau restent à faire après génération locale des
agrégats.

### 8.6 Pilote Horizon Pilier 2

L'étape facultative `innovation_horizon` calcule `INN-PRJ-01` sur les projets
Pilier 2 au statut `SIGNED`, année d'appel et tous rôles confondus. La France
est calculée sur les projets ayant au moins une participation française ;
les établissements sont rattachés à partir de ces seules participations, par
la règle commune et, lorsqu'elle est disponible, la lignée OpenAlex. Un projet
ne compte qu'une fois par entité et année. Les identifiants numériques à neuf
chiffres dont la source est absente ne sont pas interprétés comme SIREN ou PIC ;
les éventuels RNSR de ces participations restent exploitables.

Le calcul échoue si un même projet Pilier 2 `SIGNED` a des statuts ou années d'appel
contradictoires. Les projets signés sans année valide sont exclus des séries et
comptés dans les contrôles. Les résultats sont provisoires, sans ventilation
disciplinaire ni montant financier ; seules les combinaisons observées sont
écrites. Les sorties sont `innovation/horizon_effectifs.parquet`,
`horizon_entites.parquet`, `horizon_meta.json` et, localement,
`horizon_non_rattaches.parquet`. La publication Athena crée trois collections
séparées `innovation_horizon_effectifs`, `innovation_horizon_entites` et
`innovation_horizon_meta`, sans modifier les collections du pilote startups.
Leur exposition dans l'API et les vues du dépôt tableaux reste à réaliser ;
aucune publication MongoDB ni activation n'est faite automatiquement.

### 8.7 Tests et recette

Prévoir des jeux synthétiques et quelques contrôles manuels reproductibles :

- Une famille avec deux documents et deux déposants liés à deux ROR : un objet
  par ROR, un seul pour France et `MONDE` si le périmètre le permet ; CPC
  multiple non additif. Famille ou date absente mise en contrôle.
- Un projet avec deux participations d'un même ROR et une tutelle partagée :
  PRJ-01 = 1 par ROR, pas deux ; montants par participation distincte,
  jamais le budget global répété. Appel, pays et pilier vérifiés.
- Une startup multi-liée, SIREN à zéro initial, date manquante ou SIRET non
  résolu : dédoublonnage par ROR et conservation des exclusions ; statut actuel
  non extrapolé aux années précédentes.
- Une publication avec plusieurs mentions d'une même entreprise : ACK-01 = 1 ;
  DOI ambigu, texte non observable et aucune mention traités séparément ; part
  calculée sur le même univers observable, `null` si dénominateur nul.
- Un objet sans panel apparaît toutes disciplines, pas dans un panel ; aucun
  double compte d'une ligne `ALL`. Test des cumulés, unités, disponibilité,
  actualisation et provenance ; résultats identiques à partir des mêmes entrées.
- Avant livraison, contrôler manuellement au moins cinq établissements et des
  cas positifs/négatifs par source ; publier les nombres examinés et les erreurs
  observées, sans considérer cet échantillon comme preuve statistique suffisante
  de précision ou de rappel. Pour le classifieur, constituer un jeu annoté séparé
  et faire valider des seuils métier avant publication.

## 9. Décisions à obtenir

1. Confirmer le comptage entier et le périmètre d'attribution directe/indirecte,
   notamment pour les tutelles et les montants.
2. Confirmer la définition de l'année : appel Horizon, création SIREN et, pour
   les brevets, priorité ou dépôt selon disponibilité réelle.
3. Arrêter les sources, droits et critères de couverture pour chaque axe et
   pour l'international ; préciser le destinataire des éventuelles données BPI.
4. Choisir les seuils de publication et la méthode ERC pour brevets et projets
   après évaluation ; accepter l'abstention et une vue toutes disciplines.
5. Obtenir les référentiels datés laboratoire → institut, et les données RH si
   la demande initiale stock/recrutements doit être couverte.
6. Valider l'articulation avec les indicateurs COMP, les définitions Labcom,
   Carnot et Cifre, et les éventuels usages du CIR.
