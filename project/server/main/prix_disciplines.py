"""
prix_disciplines.py
===================
Discipline (panel ERC) des lauréats de prix, déduite de leurs publications dans le
snapshot OpenAlex, avec la même table topic -> discipline que la bibliométrie
(<DATA_DIR>/disciplines/<CLASSEMENT>.parquet, étape 3).

Deux étapes, après prix.compute_prix() (qui écrit prix/prix_personnes.parquet) :

1. extraire_publications_laureats()   (long : relit les works du snapshot sur S3,
   de l'ordre de l'étape 1 ; reprise possible, fichier par fichier)
   Pour chaque fichier works du snapshot (liste : <DATA_DIR>/works_manifest.json), une
   ligne par (article, auteur) dont l'auteur est un lauréat possible :
     - son ORCID OpenAlex est celui d'un lauréat (laureat_identifiant_orcid de Paysage) ;
     - ou (PRIX_DISC_NOMS) son nom normalisé (minuscules, sans accents ni ponctuation)
       est « prénom nom » ou « nom prénom » d'un lauréat.
   Mêmes filtres que l'étape 1 (types article et review, ni rétracté ni paratexte, avec un
   topic), depuis PRIX_PUB_ANNEE_MIN. Écrit prix/publications/<fichier>.parquet :
     work_id, year, topic_id, author_id, orcid, cle_nom, inst_ids
   et prix/publications_cibles.parquet (ORCID et noms cherchés). Si de nouveaux lauréats
   apparaissent, tout est relu (sinon les fichiers déjà lus sont sautés).

2. compute_prix_disciplines()          (quelques minutes, local)
   Appariement lauréat -> auteurs OpenAlex :
     orcid            : auteurs OpenAlex qui portent l'ORCID du lauréat ;
     nom_affiliation  : sinon, auteurs de même nom qui ont publié dans l'un des
                        établissements du lauréat (entites, lignée OpenAlex comprise) ;
     non_classe       : sinon.
   Publications retenues : celles des PRIX_DISC_FENETRE années jusqu'à l'année du prix
   incluse ; moins de PRIX_DISC_MIN_PUBLICATIONS -> toutes celles du snapshot.
   Discipline : part des publications par discipline (topic principal -> panel) ;
   discipline_code = la plus fréquente (à égalité : code le plus petit), domaine_code = son
   domaine (LS, PE, SH).
   Écrit dans prix/ :
     prix_laureats_disciplines.parquet  laureat_id, discipline_code, domaine_code,
                                        disciplines [{discipline_code, poids}],
                                        methode_discipline, nb_publications_discipline,
                                        auteurs_openalex
     prix_effectifs_disciplines.parquet entite_type x entite_id x annee x prix_id x
                                        discipline_code : nb_laureats (discipline principale,
                                        entier), nb_laureats_fractionnaire (poids)
     prix_disciplines_meta.json         couverture par méthode, paramètres (fusionné dans
                                        prix_meta à la publication)

Exemple :
    from project.server.main.prix_disciplines import extraire_publications_laureats, compute_prix_disciplines
    extraire_publications_laureats()          # long, reprise possible
    compute_prix_disciplines()
"""

import datetime as dt
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import pandas as pd

from .config import (CLASSEMENT, DATA_DIR, PRIX_DIR, PRIX_DISC_FENETRE, PRIX_DISC_MIN_PUBLICATIONS,
                     PRIX_DISC_NOMS, PRIX_PUB_ANNEE_MIN)

ORCID_RE = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])")
ORCID_SQL = r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])"
COLS_PUB = ["work_id", "year", "topic_id", "author_id", "orcid", "cle_nom", "inst_ids"]


def _cle_sql(expr: str) -> str:
    """Nom normalisé, même règle pour les lauréats et les auteurs OpenAlex (toujours en SQL) :
    'Jean-Éric  DUPONT' -> 'jean eric dupont'."""
    return f"trim(regexp_replace(lower(strip_accents(coalesce({expr}, ''))), '[^a-z]+', ' ', 'g'))"


def _short(expr: str) -> str:
    return f"regexp_extract({expr}, '[^/]+$')"


def _orcid(v):
    if not isinstance(v, str):
        return None
    m = ORCID_RE.search(v.upper())
    return m.group(1) if m else None


# --- Cibles ------------------------------------------------------------------------------

def cibles(prix_dir: str = PRIX_DIR, noms: bool = PRIX_DISC_NOMS) -> tuple:
    """(orcids, cles_noms) : DataFrames des ORCID et des noms normalisés à chercher,
    à partir de prix/prix_personnes.parquet (prix.compute_prix())."""
    f = Path(prix_dir) / "prix_personnes.parquet"
    if not f.exists():
        raise FileNotFoundError(f"{f} absent : lancer prix.compute_prix() d'abord")
    p = duckdb.sql(f"SELECT * FROM read_parquet('{f}')").df()
    orcids = pd.DataFrame({"orcid": sorted({o for o in map(_orcid, p["orcid"]) if isinstance(o, str)})})
    if not noms:
        return orcids, pd.DataFrame({"cle_nom": pd.Series([], dtype=str)})
    con = duckdb.connect()
    con.register("p", p[["prenom", "nom"]].dropna())
    cles = con.execute(f"""
        SELECT DISTINCT cle_nom FROM (
            SELECT {_cle_sql("prenom || ' ' || nom")} AS cle_nom FROM p
            UNION ALL SELECT {_cle_sql("nom || ' ' || prenom")} FROM p)
        WHERE length(cle_nom) >= 5 AND cle_nom LIKE '% %' ORDER BY 1
    """).df()
    con.close()
    return orcids, cles


# --- 1. Extraction ---------------------------------------------------------------------------

def _sql_publications(url: str, annee_min: int, types: tuple) -> str:
    types_sql = ", ".join(f"'{t}'" for t in types)
    # Comme l'étape 1 : on ne cite que des sous-champs précis des auteurs (list_transform),
    # pour que DuckDB ne lise que ces colonnes du Parquet distant ; puis une ligne par auteur
    # (unnest), gardée si son ORCID ou son nom est cherché (jointures sur les petites tables
    # c_orcid et c_noms).
    return f"""
    WITH w AS (
        SELECT {_short('id')} AS work_id, publication_year::SMALLINT AS year,
               {_short('primary_topic.id')} AS topic_id,
               unnest(list_transform(authorships, a -> {{
                   'author_id': {_short('a.author.id')},
                   'orcid': regexp_extract(upper(coalesce(a.author.orcid::VARCHAR, '')), '{ORCID_SQL}', 1),
                   'nom': a.author.display_name,
                   'inst_ids': list_filter(list_transform(a.institutions, i -> {_short('i.id')}),
                                           x -> x IS NOT NULL AND x <> '')
               }})) AS au
        FROM read_parquet('{url}')
        WHERE publication_year >= {int(annee_min)}
          AND type IN ({types_sql})
          AND NOT coalesce(is_retracted, false)
          AND NOT coalesce(is_paratext, false)
          AND primary_topic.id IS NOT NULL
    ), a AS (
        SELECT work_id, year, topic_id, au.author_id AS author_id, nullif(au.orcid, '') AS orcid,
               {_cle_sql('au.nom')} AS cle_nom, au.inst_ids AS inst_ids
        FROM w
    )
    SELECT * FROM a
    WHERE orcid IN (SELECT orcid FROM c_orcid) OR cle_nom IN (SELECT cle_nom FROM c_noms)
    """


def _connexion(url: str):
    from .openalex_s3 import connect_for
    return connect_for(url)


def _nom_fichier(url: str) -> str:
    parts = url.split("/")
    return f"{parts[-2]}__{parts[-1]}"


def _extraire_un(url: str, out_dir: Path, c_orcid: pd.DataFrame, c_noms: pd.DataFrame,
                 annee_min: int, types: tuple, retries: int = 3) -> tuple:
    from .openalex_s3 import readable_url
    f = out_dir / _nom_fichier(url)
    if f.exists():
        return f.name, -1, 0.0
    for essai in range(1, retries + 1):
        try:
            t0 = time.time()
            con = _connexion(url)
            con.register("c_orcid", c_orcid)
            con.register("c_noms", c_noms)
            tmp = f.with_suffix(".tmp")
            con.execute(f"COPY ({_sql_publications(readable_url(url), annee_min, types)}) "
                        f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
            n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            con.close()
            tmp.rename(f)
            return f.name, n, time.time() - t0
        except Exception as e:  # réseau, timeout S3...
            if essai == retries:
                raise RuntimeError(f"{f.name} : échec après {retries} essais : {e}") from e
            time.sleep(5 * essai)


def extraire_publications_laureats(prix_dir: str = PRIX_DIR, data_dir: str = DATA_DIR,
                                   annee_min: int = PRIX_PUB_ANNEE_MIN, noms: bool = PRIX_DISC_NOMS,
                                   types: tuple = ("article", "review"), workers: int = 8,
                                   urls: list = None, verbose: bool = True) -> dict:
    """Publications OpenAlex des lauréats possibles -> <prix_dir>/publications/ (voir l'en-tête).
    urls : fichiers works à lire (défaut : ceux de <data_dir>/works_manifest.json)."""
    out = Path(prix_dir) / "publications"
    out.mkdir(parents=True, exist_ok=True)
    c_orcid, c_noms = cibles(prix_dir, noms)
    if urls is None:
        man = Path(data_dir) / "works_manifest.json"
        if not man.exists():
            raise FileNotFoundError(f"{man} absent : extraire le snapshot d'abord (extract_snapshot)")
        urls = [f["url"] for f in json.loads(man.read_text())["files"]]

    # Nouvelles cibles depuis la dernière extraction : tout relire (les fichiers déjà lus
    # ne contiennent pas leurs publications)
    f_cib = Path(prix_dir) / "publications_cibles.parquet"
    params = {"annee_min": int(annee_min), "types": list(types)}
    f_par = out / "params.json"
    if f_cib.exists() and f_par.exists():
        old = duckdb.sql(f"SELECT * FROM read_parquet('{f_cib}')").df()
        nouveaux = (set(c_orcid["orcid"]) - set(old.loc[old["type"] == "orcid", "valeur"])) | \
                   (set(c_noms["cle_nom"]) - set(old.loc[old["type"] == "nom", "valeur"]))
        if nouveaux or json.loads(f_par.read_text()) != params:
            if verbose:
                print(f"{len(nouveaux):,} nouvelles cibles ou paramètres changés : publications relues")
            for p in out.glob("*.parquet"):
                p.unlink()
    for p in out.glob("*.tmp"):
        p.unlink()
    cib = pd.concat([pd.DataFrame({"type": "orcid", "valeur": c_orcid["orcid"]}),
                     pd.DataFrame({"type": "nom", "valeur": c_noms["cle_nom"]})], ignore_index=True)
    with duckdb.connect() as con:
        con.register("cib", cib)
        con.execute(f"COPY cib TO '{f_cib}' (FORMAT parquet)")
    f_par.write_text(json.dumps(params))

    if verbose:
        faits = sum((out / _nom_fichier(u)).exists() for u in urls)
        print(f"Publications des lauréats : {len(c_orcid):,} ORCID, {len(c_noms):,} noms cherchés ; "
              f"{len(urls)} fichiers works, {faits} déjà lus")
    if urls and str(urls[0]).startswith("s3://"):
        from .openalex_s3 import connect
        connect(install=True).close()     # httpfs installé une fois, avant les threads
    t0 = time.time()
    res = {"fichiers": len(urls), "lus": 0, "deja_faits": 0, "lignes": 0, "erreurs": []}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(_extraire_un, u, out, c_orcid, c_noms, annee_min, tuple(types)) for u in urls]
        for k, fut in enumerate(as_completed(futs), 1):
            try:
                nom, n, sec = fut.result()
                if n < 0:
                    res["deja_faits"] += 1
                    msg = "déjà lu"
                else:
                    res["lus"] += 1
                    res["lignes"] += n
                    msg = f"{n:>8,} lignes en {sec:5.1f} s"
            except Exception as e:
                res["erreurs"].append(str(e))
                nom, msg = "", f"ERREUR {e}"
            if verbose:
                print(f"[{k}/{len(urls)}] {nom}  {msg}")
    res["minutes"] = round((time.time() - t0) / 60, 1)
    if verbose:
        print(f"Terminé en {res['minutes']} min — {res['lus']} lus, {res['deja_faits']} déjà faits, "
              f"{len(res['erreurs'])} erreur(s)")
    return res


# --- 2. Calcul -------------------------------------------------------------------------------

def compute_prix_disciplines(prix_dir: str = PRIX_DIR, data_dir: str = DATA_DIR,
                             classement: str = CLASSEMENT, fenetre: int = PRIX_DISC_FENETRE,
                             min_publications: int = PRIX_DISC_MIN_PUBLICATIONS,
                             noms: bool = PRIX_DISC_NOMS, verbose: bool = True) -> dict:
    """Discipline de chaque lauréat et effectifs par discipline (voir l'en-tête)."""
    prix_dir, data_dir = Path(prix_dir), Path(data_dir)
    f_pers = prix_dir / "prix_personnes.parquet"
    f_eff = prix_dir / "prix_effectifs.parquet"
    f_disc = data_dir / "disciplines" / f"{classement}.parquet"
    pubs = prix_dir / "publications"
    for f in (f_pers, f_eff, f_disc):
        if not f.exists():
            raise FileNotFoundError(f"{f} absent (prix.compute_prix(), disciplines.build_disciplines())")
    if not any(pubs.glob("*.parquet")):
        raise FileNotFoundError(f"{pubs} vide : lancer extraire_publications_laureats()")

    con = duckdb.connect()
    pers = con.execute(f"SELECT * FROM read_parquet('{f_pers}')").df()
    pers["orcid"] = [_orcid(v) for v in pers["orcid"]]
    con.register("pers_df", pers[["laureat_id", "annee", "prenom", "nom", "orcid", "entites"]])
    con.execute(f"""
        CREATE TEMP TABLE pers AS
        SELECT laureat_id, annee::INTEGER AS annee, orcid, entites,
               {_cle_sql("prenom || ' ' || nom")} AS cle1, {_cle_sql("nom || ' ' || prenom")} AS cle2
        FROM pers_df
    """)
    con.execute(f"CREATE TEMP TABLE pub AS SELECT DISTINCT * FROM read_parquet('{pubs}/*.parquet')")

    # Lauréat -> auteurs OpenAlex : ORCID d'abord
    con.execute("""
        CREATE TEMP TABLE match_orcid AS
        SELECT DISTINCT p.laureat_id, b.author_id, 'orcid' AS methode
        FROM pers p JOIN (SELECT DISTINCT orcid, author_id FROM pub WHERE orcid IS NOT NULL) b
          ON p.orcid = b.orcid
    """)
    # ... sinon même nom et une publication dans l'un des établissements du lauréat
    lin = data_dir / "inst_lineage.parquet"
    if noms and lin.exists():
        con.execute(f"""
            CREATE TEMP TABLE match_nom AS
            WITH cand AS (
                SELECT p.laureat_id, p.entites, b.author_id, b.inst_ids
                FROM pers p JOIN pub b ON b.cle_nom IN (p.cle1, p.cle2)
                WHERE p.laureat_id NOT IN (SELECT laureat_id FROM match_orcid)
                  AND len(p.entites) > 0
            ), ci AS (SELECT laureat_id, entites, author_id, unnest(inst_ids) AS inst_id FROM cand)
            SELECT DISTINCT ci.laureat_id, ci.author_id, 'nom_affiliation' AS methode
            FROM ci JOIN read_parquet('{lin}') l ON l.inst_id = ci.inst_id
            WHERE list_contains(ci.entites, l.anc_ror)
        """)
    else:
        con.execute("CREATE TEMP TABLE match_nom AS SELECT * FROM match_orcid LIMIT 0")
    con.execute("CREATE TEMP TABLE m AS SELECT * FROM match_orcid UNION ALL SELECT * FROM match_nom")

    # Publications de ces auteurs, fenêtre avant le prix, sinon toutes
    con.execute(f"""
        CREATE TEMP TABLE w AS
        SELECT DISTINCT m.laureat_id, b.work_id, b.year, d.discipline_code, d.domaine_code,
               b.year BETWEEN p.annee - {int(fenetre) - 1} AND p.annee AS dans_fenetre
        FROM m JOIN pub b USING (author_id) JOIN pers p USING (laureat_id)
        JOIN read_parquet('{f_disc}') d ON d.topic_id = b.topic_id
        WHERE d.discipline_code IS NOT NULL
    """)
    con.execute(f"""
        CREATE TEMP TABLE w_ok AS
        WITH n AS (SELECT laureat_id, count(DISTINCT work_id) FILTER (WHERE dans_fenetre) AS n_f
                   FROM w GROUP BY 1)
        SELECT w.* FROM w JOIN n USING (laureat_id)
        WHERE w.dans_fenetre OR n.n_f < {int(min_publications)}
    """)
    poids = con.execute("""
        WITH c AS (SELECT laureat_id, discipline_code, any_value(domaine_code) AS domaine_code,
                          count(DISTINCT work_id) AS n FROM w_ok GROUP BY 1, 2)
        SELECT laureat_id, discipline_code, domaine_code,
               n / sum(n) OVER (PARTITION BY laureat_id) AS poids,
               sum(n) OVER (PARTITION BY laureat_id) AS nb_publications
        FROM c
    """).df()
    meth = con.execute("""SELECT laureat_id, CASE WHEN bool_or(methode = 'orcid') THEN 'orcid'
                                 ELSE 'nom_affiliation' END AS methode_discipline,
                                 list(DISTINCT author_id ORDER BY author_id) AS auteurs_openalex
                          FROM m GROUP BY 1""").df()

    # Une ligne par lauréat
    poids = poids.sort_values(["laureat_id", "poids", "discipline_code"], ascending=[True, False, True])
    princ = poids.drop_duplicates("laureat_id")[["laureat_id", "discipline_code", "domaine_code",
                                                 "nb_publications"]]
    listes = (poids.assign(poids=poids["poids"].round(4))
              .groupby("laureat_id")[["discipline_code", "poids"]]
              .apply(lambda g: g.to_dict("records")).rename("disciplines"))
    ld = pers[["laureat_id"]].merge(princ, on="laureat_id", how="left").merge(meth, on="laureat_id", how="left")
    ld["disciplines"] = ld["laureat_id"].map(listes).map(lambda v: v if isinstance(v, list) else [])
    classe = ld["discipline_code"].notna()
    ld["methode_discipline"] = ld["methode_discipline"].where(classe, "non_classe")
    ld["nb_publications_discipline"] = ld.pop("nb_publications").fillna(0).astype("int64")
    ld["auteurs_openalex"] = ld["auteurs_openalex"].map(lambda v: [] if isinstance(v, float) or v is None else list(v))
    ld = ld[["laureat_id", "discipline_code", "domaine_code", "disciplines", "methode_discipline",
             "nb_publications_discipline", "auteurs_openalex"]]
    con.register("ld", ld)
    con.execute(f"COPY ld TO '{prix_dir / 'prix_laureats_disciplines.parquet'}' (FORMAT parquet)")

    # Effectifs par discipline, au grain de prix_effectifs (+ discipline_code) : on reprend
    # les mêmes lauréats par entité (prix_personnes.entites + FR)
    con.register("poids_df", poids)
    eff = con.execute(f"""
        WITH lp AS (
            SELECT laureat_id, annee, prix_id, 'pays' AS entite_type, 'FR' AS entite_id
            FROM read_parquet('{f_pers}')
            UNION ALL
            SELECT laureat_id, annee, prix_id, 'etablissement', unnest(entites)
            FROM read_parquet('{f_pers}')
        ), princ AS (
            SELECT laureat_id, discipline_code FROM ld WHERE discipline_code IS NOT NULL
        )
        SELECT lp.entite_type, lp.entite_id, lp.annee::INTEGER AS annee, lp.prix_id, pd.discipline_code,
               count(DISTINCT lp.laureat_id) FILTER (WHERE pr.laureat_id IS NOT NULL)::BIGINT AS nb_laureats,
               round(sum(pd.poids), 4) AS nb_laureats_fractionnaire
        FROM lp JOIN poids_df pd USING (laureat_id)
        LEFT JOIN princ pr ON pr.laureat_id = lp.laureat_id AND pr.discipline_code = pd.discipline_code
        WHERE lp.annee IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY CASE entite_type WHEN 'pays' THEN 0 ELSE 1 END, 2, 3, 4, 5
    """).df()
    con.register("eff", eff)
    con.execute(f"COPY eff TO '{prix_dir / 'prix_effectifs_disciplines.parquet'}' (FORMAT parquet)")
    con.close()

    personnes = pers.get("laureat_type", pd.Series("Personne", index=pers.index)).eq("Personne")
    n_pers = int(personnes.sum())
    par_methode = ld.loc[personnes.values, "methode_discipline"].value_counts().to_dict()
    meta = {
        "date_calcul": dt.date.today().isoformat(),
        "classement": classement,
        "parametres": {"fenetre": fenetre, "min_publications": min_publications, "noms": noms,
                       "annee_min_publications": PRIX_PUB_ANNEE_MIN},
        "controles": {
            "nb_laureats_personnes": n_pers,
            "nb_avec_orcid": int(pers.loc[personnes, "orcid"].notna().sum()),
            "par_methode": par_methode,
            "part_classes": round(float(ld.loc[personnes.values, "discipline_code"].notna().mean())
                                  if n_pers else 0.0, 4),
            "par_domaine": ld["domaine_code"].fillna("(non classé)").value_counts().to_dict(),
        },
        "methode": [
            "Discipline des lauréats : panel ERC de leurs publications (topic principal OpenAlex, même "
            "classement que la bibliométrie). Auteur OpenAlex retrouvé par l'ORCID du lauréat, à "
            "défaut par son nom et une publication dans l'un de ses établissements.",
            f"Publications des {fenetre} années précédant le prix (année du prix comprise) ; moins de "
            f"{min_publications} : toutes les publications depuis {PRIX_PUB_ANNEE_MIN}.",
            "Discipline principale : le panel le plus fréquent ; effectifs fractionnaires : part de "
            "chaque panel dans les publications du lauréat.",
        ],
    }
    (prix_dir / "prix_disciplines_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1),
                                                          encoding="utf-8")
    if verbose:
        c = meta["controles"]
        print(f"Disciplines des lauréats : {c['part_classes']:.1%} des {n_pers:,} personnes classées ; "
              f"méthodes {c['par_methode']} ; {c['nb_avec_orcid']:,} avec un ORCID")
        print(f"  domaines : {c['par_domaine']}")
    return meta
