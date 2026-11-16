"""
publications_auteurs.py
=======================
Code commun aux lauréats de prix (prix_disciplines.py) et aux viviers de chercheurs
(viviers.py) :

1. extraire_publications()   (long : relit les works du snapshot sur S3, de l'ordre de
   l'étape 1 ; reprise possible, fichier par fichier)
   UNE SEULE lecture des works pour les deux usages : chaque fichier works est lu une fois
   et écrit, selon ce qui manque,
     prix/publications/<fichier>.parquet   publications des lauréats possibles (ORCID ou
                                           nom cherché), depuis PRIX_PUB_ANNEE_MIN
     viviers/auteurs/<fichier>.parquet     signatures dans les pays des viviers, depuis
                                           VIVIER_PUB_ANNEE_MIN (articles, reviews, livres et
                                           chapitres : VIVIER_PUB_TYPES)
   Chaque sortie garde ses paramètres (params.json) et sa reprise : si les lauréats changent,
   seules les publications des prix sont relues (les fichiers des viviers déjà écrits sont
   gardés), et inversement si les pays ou la période des viviers changent.
   extraire_publications_laureats() et extraire_auteurs() n'en lancent qu'une.

2. poids_disciplines()       règle de discipline commune : part des publications par panel
   ERC (topic principal -> panel, table disciplines/<CLASSEMENT>.parquet), discipline
   principale = la plus fréquente (à égalité : code le plus petit).

Exemple :
    from project.server.main.publications_auteurs import extraire_publications
    extraire_publications()          # prix et viviers en une lecture
"""

import json
from pathlib import Path

import duckdb
import pandas as pd

from .common import pays_list_sql, pays_sql
from .config import (DATA_DIR, ERC_DIR, PRIX_DIR, PRIX_DISC_NOMS, PRIX_PUB_ANNEE_MIN,
                     VIVIER_PAYS, VIVIER_PUB_ANNEE_MIN, VIVIER_PUB_TYPES, VIVIERS_DIR)
from .openalex_s3 import extract_file, out_name as _nom_fichier, readable_url, run_files, short_id as _short

TYPES = ("article", "review")      # types lus pour les prix
# Version du schéma de viviers/auteurs : l'augmenter quand des colonnes changent (relecture
# des works pour les viviers). 2 : type, topic_id.
VERSION_VIVIERS = 2
ORCID_SQL = r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])"


def _cle_sql(expr: str) -> str:
    """Nom normalisé, même règle pour les lauréats et les auteurs OpenAlex (toujours en SQL) :
    'Jean-Éric  DUPONT' -> 'jean eric dupont'."""
    return f"trim(regexp_replace(lower(strip_accents(coalesce({expr}, ''))), '[^a-z]+', ' ', 'g'))"


# --- Règle de discipline -----------------------------------------------------------------------

def poids_disciplines(con, source: str, cle: str, f_disc, table: str = "poids") -> None:
    """Crée la table `table` : une ligne par (cle, discipline_code) avec domaine_code,
    n (publications distinctes), nb_publications (toutes disciplines), poids (n / total),
    principale (panel le plus fréquent ; à égalité, code le plus petit).
    source : relation SQL (table ou sous-requête) avec les colonnes <cle>, work_id,
    discipline_code (lignes sans discipline ignorées)."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table} AS
        WITH c AS (
            SELECT {cle}, discipline_code, count(DISTINCT work_id) AS n
            FROM {source} WHERE discipline_code IS NOT NULL GROUP BY 1, 2
        ), d AS (SELECT DISTINCT discipline_code, domaine_code FROM read_parquet('{f_disc}'))
        SELECT c.{cle}, c.discipline_code, d.domaine_code, c.n,
               sum(c.n) OVER (PARTITION BY c.{cle}) AS nb_publications,
               c.n / sum(c.n) OVER (PARTITION BY c.{cle}) AS poids,
               row_number() OVER (PARTITION BY c.{cle} ORDER BY c.n DESC, c.discipline_code) = 1
                   AS principale
        FROM c LEFT JOIN d USING (discipline_code)
    """)


# --- Extraction commune ------------------------------------------------------------------------

def _sql_signatures(url: str, types: tuple, annee_min: int, prix: bool, viviers: bool, pays) -> str:
    # types : union des types des sorties demandées ; chaque sortie refiltre les siens
    """Une ligne par (article, auteur) utile à l'un des deux usages. Comme l'étape 1 : seuls
    des sous-champs précis des auteurs sont cités (list_transform), pour que DuckDB ne lise que
    ces colonnes du Parquet distant ; position, correspondant et pays ne sont lus que pour les
    viviers."""
    types_sql = ", ".join(f"'{t}'" for t in types)
    champs = [f"'author_id': {_short('a.author.id')}",
              f"'orcid': regexp_extract(upper(coalesce(a.author.orcid::VARCHAR, '')), '{ORCID_SQL}', 1)",
              "'nom': a.author.display_name",
              f"'inst_ids': list_filter(list_transform(a.institutions, i -> {_short('i.id')}), "
              "x -> x IS NOT NULL AND x <> '')"]
    if viviers:
        lus = pays_list_sql(sorted(set(pays) - {"FR"}) or ["--"])
        champs += ["'position_auteur': a.author_position",
                   "'correspondant': a.is_corresponding",
                   f"""'pays': list_distinct(list_filter(list_transform(a.institutions, i ->
                           {pays_sql('i.country_code')}),
                           c -> c = 'FR' OR c IN ({lus})))"""]
        en_plus = ("au.position_auteur AS position_auteur, coalesce(au.correspondant, false) AS correspondant, "
                   "au.pays AS pays")
    else:
        en_plus = ("NULL::VARCHAR AS position_auteur, false AS correspondant, "
                   "[]::VARCHAR[] AS pays")
    return f"""
    WITH w AS (
        SELECT {_short('id')} AS work_id, publication_year::SMALLINT AS year, type,
               {_short('primary_topic.id')} AS topic_id, len(authorships)::INTEGER AS nb_auteurs,
               unnest(list_transform(authorships, a -> {{{', '.join(champs)}}})) AS au
        FROM read_parquet('{url}')
        WHERE publication_year >= {int(annee_min)}
          AND type IN ({types_sql})
          AND NOT coalesce(is_retracted, false)
          AND NOT coalesce(is_paratext, false)
          AND primary_topic.id IS NOT NULL
    )
    SELECT work_id, year, type, topic_id, nb_auteurs, au.author_id AS author_id,
           nullif(au.orcid, '') AS orcid, au.nom AS nom, {_cle_sql('au.nom')} AS cle_nom,
           au.inst_ids AS inst_ids, {en_plus}
    FROM w
    """


def _types_sql(types) -> str:
    return ", ".join(f"'{t}'" for t in types)


def _cond_prix(c: dict) -> str:
    return (f"year >= {int(c['annee_min'])} AND type IN ({_types_sql(c['types'])}) "
            f"AND (orcid IN (SELECT orcid FROM c_orcid) OR cle_nom IN (SELECT cle_nom FROM c_noms))")


def _cond_viviers(c: dict) -> str:
    return (f"year >= {int(c['annee_min'])} AND type IN ({_types_sql(c['types'])}) "
            f"AND author_id IS NOT NULL AND author_id <> '' AND len(pays) > 0")


SELECT_PRIX = "work_id, year, topic_id, author_id, orcid, cle_nom, inst_ids"
# Institutions gardées seulement pour les signatures françaises (établissements)
SELECT_VIVIERS = ("work_id, year, type, topic_id, nb_auteurs, author_id, orcid, nom, position_auteur, correspondant, "
                  "pays, "
                  "CASE WHEN list_contains(pays, 'FR') THEN inst_ids END AS inst_ids")


def _extraire_un(url: str, cotes: dict, retries: int = 3) -> dict:
    """Lit un fichier works une fois et écrit les sorties manquantes (cotes : nom -> réglages).
    Renvoie {sortie: nb lignes}, {} si rien ne manquait."""
    manquants = {k: c for k, c in cotes.items() if not (c["out"] / _nom_fichier(url)).exists()}
    if not manquants:
        return {}
    p, v = manquants.get("prix"), manquants.get("viviers")
    annee_min = min(c["annee_min"] for c in manquants.values())
    types = tuple(sorted({t for c in manquants.values() for t in c["types"]}))
    conds = [f"({_cond_prix(p)})"] if p else []
    conds += [f"({_cond_viviers(v)})"] if v else []

    def prepare(con):
        if p:
            con.register("c_orcid", p["c_orcid"])
            con.register("c_noms", p["c_noms"])

    sql = _sql_signatures(readable_url(url), types, annee_min, bool(p), bool(v), v["pays"] if v else None)
    outputs = {c["out"] / _nom_fichier(url):
               f"SELECT {SELECT_PRIX if k == 'prix' else SELECT_VIVIERS} FROM t "
               f"WHERE {_cond_prix(c) if k == 'prix' else _cond_viviers(c)}"
               for k, c in manquants.items()}
    lignes = extract_file(url, f"SELECT * FROM ({sql}) WHERE {' OR '.join(conds)}", outputs,
                          prepare=prepare, retries=retries)
    # sorties nommées comme les cotes (dossiers publications/ et auteurs/)
    noms = {c["out"].name: k for k, c in manquants.items()}
    return {noms[d]: n for d, n in lignes.items()}


def _preparer_prix(prix_dir: Path, annee_min: int, noms: bool, types: tuple, verbose: bool) -> dict:
    """Sortie prix/publications : cibles, et tout relire si de nouveaux lauréats apparaissent
    (les fichiers déjà lus ne contiennent pas leurs publications)."""
    from .prix_disciplines import cibles
    out = prix_dir / "publications"
    out.mkdir(parents=True, exist_ok=True)
    c_orcid, c_noms = cibles(str(prix_dir), noms)
    f_cib = prix_dir / "publications_cibles.parquet"
    params = {"annee_min": int(annee_min), "types": list(types)}
    f_par = out / "params.json"
    if f_cib.exists() and f_par.exists():
        old = duckdb.sql(f"SELECT * FROM read_parquet('{f_cib}')").df()
        nouveaux = (set(c_orcid["orcid"]) - set(old.loc[old["type"] == "orcid", "valeur"])) | \
                   (set(c_noms["cle_nom"]) - set(old.loc[old["type"] == "nom", "valeur"]))
        if nouveaux or json.loads(f_par.read_text()) != params:
            if verbose:
                print(f"Prix : {len(nouveaux):,} nouvelles cibles ou paramètres changés : publications relues")
            for f in out.glob("*.parquet"):
                f.unlink()
    elif f_par.exists() and json.loads(f_par.read_text()) != params:
        for f in out.glob("*.parquet"):
            f.unlink()
    cib = pd.concat([pd.DataFrame({"type": "orcid", "valeur": c_orcid["orcid"]}),
                     pd.DataFrame({"type": "nom", "valeur": c_noms["cle_nom"]})], ignore_index=True)
    with duckdb.connect() as con:
        con.register("cib", cib)
        con.execute(f"COPY cib TO '{f_cib}' (FORMAT parquet)")
    f_par.write_text(json.dumps(params))
    if verbose:
        print(f"Prix : {len(c_orcid):,} ORCID, {len(c_noms):,} noms cherchés, publications depuis {annee_min}")
    return {"out": out, "annee_min": int(annee_min), "types": list(types), "c_orcid": c_orcid, "c_noms": c_noms}


def _preparer_viviers(viviers_dir: Path, data_dir: str, erc_dir: str, annee_min: int, pays,
                      types: tuple, verbose: bool) -> dict:
    """Sortie viviers/auteurs : pays lus, et tout relire si les paramètres changent."""
    from .viviers import pays_viviers
    out = viviers_dir / "auteurs"
    out.mkdir(parents=True, exist_ok=True)
    pays = pays_viviers(erc_dir, VIVIER_PAYS if pays is None else pays, data_dir, verbose=verbose)
    params = {"annee_min": int(annee_min), "types": list(types), "pays": pays, "version": VERSION_VIVIERS}
    f_par = out / "params.json"
    if f_par.exists() and json.loads(f_par.read_text()) != params:
        if verbose:
            print("Viviers : paramètres d'extraction changés : signatures relues")
        for f in out.glob("*.parquet"):
            f.unlink()
    f_par.write_text(json.dumps(params))
    if verbose:
        print(f"Viviers : signatures de {len(pays)} pays ({', '.join(pays)}) depuis {annee_min}")
    return {"out": out, "annee_min": int(annee_min), "types": list(types), "pays": pays}


def extraire_publications(prix: bool = True, viviers: bool = True,
                          prix_dir: str = PRIX_DIR, viviers_dir: str = VIVIERS_DIR,
                          data_dir: str = DATA_DIR, erc_dir: str = ERC_DIR,
                          prix_annee_min: int = PRIX_PUB_ANNEE_MIN, noms: bool = PRIX_DISC_NOMS,
                          vivier_annee_min: int = VIVIER_PUB_ANNEE_MIN, pays=None,
                          prix_types: tuple = TYPES, vivier_types: tuple = VIVIER_PUB_TYPES,
                          workers: int = 8, urls: list = None,
                          verbose: bool = True) -> dict:
    """Publications des lauréats (prix) et signatures des viviers en une seule lecture des works
    (voir l'en-tête). prix / viviers : sorties demandées. pays : défaut viviers.pays_viviers().
    urls : fichiers works à lire (défaut : ceux de <data_dir>/works_manifest.json)."""
    if urls is None:
        man = Path(data_dir) / "works_manifest.json"
        if not man.exists():
            raise FileNotFoundError(f"{man} absent : extraire le snapshot d'abord (extract_snapshot)")
        urls = [f["url"] for f in json.loads(man.read_text())["files"]]
    cotes = {}
    if prix:
        cotes["prix"] = _preparer_prix(Path(prix_dir), prix_annee_min, noms, prix_types, verbose)
    if viviers:
        cotes["viviers"] = _preparer_viviers(Path(viviers_dir), data_dir, erc_dir, vivier_annee_min,
                                             pays, vivier_types, verbose)
    if not cotes:
        raise ValueError("ni prix ni viviers demandés")
    for c in cotes.values():
        for f in c["out"].glob("*.tmp"):
            f.unlink()

    a_lire = [u for u in urls if any(not (c["out"] / _nom_fichier(u)).exists() for c in cotes.values())]
    if verbose:
        print(f"{len(urls)} fichiers works, {len(urls) - len(a_lire)} déjà lus pour {' et '.join(cotes)}")
    r = run_files(a_lire, lambda u: _extraire_un(u, cotes), workers=workers, verbose=verbose)
    res = {"fichiers": len(urls), "lus": r["traites"], "deja_faits": len(urls) - len(a_lire) + r["deja_faits"],
           "lignes": {k: r["lignes"].get(k, 0) for k in cotes}, "erreurs": r["erreurs"],
           "minutes": r["minutes"]}
    return res
