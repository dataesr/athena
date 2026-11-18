"""
extract_awards.py
=================
Financements OpenAlex (entité « awards » : ANR, NIH, DFG...) par pays du financeur et
par panel ERC. Même principe que l'étape 1 (extract_works) : lecture fichier par fichier
du snapshot, sous-colonnes utiles seulement, reprise sur erreur.

Snapshot : celui des awards est INDÉPENDANT de celui des works. Par défaut
(AWARDS_SNAPSHOT = None), le snapshot quotidien le plus récent de l'offre payante
(SNAPSHOT_SOURCE = "daily", clé OPENALEX_API_KEY) ; awards et funders sont lus à la même date.
Chaque snapshot d'awards a son dossier <AWARDS_ROOT>/<date>/ (AWARDS_DIR ci-dessous) ; seul
la nomenclature topic -> panel vient de DATA_DIR (étape 3, même fichier de correspondance).

1. extract_awards()   lecture du snapshot (awards : ~230 fichiers ; funders : petite table)
     <AWARDS_DIR>/awards_manifest.json    fichiers awards du snapshot (reprise)
     <AWARDS_DIR>/funders.parquet         une ligne par financeur
         funder_id, display_name, country_code, ror, doi, alternate_titles, awards_count
     <AWARDS_DIR>/extraits/<fichier>.parquet   une ligne par award
         award_id, funder_id, funder_award_id, funding_type, funder_scheme, provenance,
         amount, currency, start_year, end_year, topic_id, topic_score,
         inst_ids, inst_countries (institutions bénéficiaires, pays OpenAlex bruts),
         lead_orcid, funded_outputs_count
   Aucun filtre sur le topic : les awards sans topic sont gardés (contrôle de couverture).

2. compute_awards()   pays du financeur x panel ERC (lecture locale seulement)
     <AWARDS_DIR>/awards_<nomenclature>.parquet   une ligne par award, enrichie :
         award_id, financeur_id, financeur_nom, pays_code, annee_debut, topic_id,
         discipline_code, domaine_code, doute, montant, devise, funding_type
     <AWARDS_DIR>/awards_pays_<nomenclature>.parquet
         pays_code, annee_debut, discipline_code, domaine_code, nb_awards,
         nb_awards_avec_montant, devise_principale, montant_devise_principale
   Règles communes avec la bibliométrie :
     - pays : common.pays_sql (outre-mer -> FR, PAYS_FR) ; financeurs internationaux
       (AWARDS_FINANCEURS_INTERNATIONAUX) sous AWARDS_PAYS_INTERNATIONAL ;
     - discipline : topic principal -> disciplines/<nomenclature>.parquet (étape 3), un topic =
       un panel ; discipline_code NULL = award sans topic ou topic non classé.
   Les montants sont dans la devise du financeur (pas de conversion dans le snapshot) :
   ils ne sont sommés que dans la devise principale de chaque pays.

Exemple :
    from project.server.main.extract_awards import extract_awards, compute_awards
    extract_awards(test=True)        # un seul fichier, pour essai
    extract_awards()                 # dernier snapshot quotidien (reprise possible)
    extract_awards("2026-11-10")     # un snapshot précis
    list_awards_snapshots()          # snapshots d'awards extraits localement
    res = compute_awards()           # le plus récent extrait (ou AWARDS_SNAPSHOT)
    res["financeurs"].query("pays_code == 'FR'").head(20)
    res["couverture"]                # part des awards avec topic / panel, par pays
"""

import json
from pathlib import Path

from .common import _sql_in, connect as local_connect, pays_sql
from .config import (AWARDS_ANNEE_MIN, AWARDS_FINANCEURS_INTERNATIONAUX, AWARDS_PAYS_INTERNATIONAL,
                     AWARDS_ROOT, AWARDS_SNAPSHOT, NOMENCLATURE_DISCIPLINAIRE, DATA_DIR, SNAPSHOT_SOURCE)
from .openalex_s3 import (connect, entity_manifest, extract_file, latest_snapshot_date, out_name,
                          readable_url, run_files, short_id as _short)
from .disciplines import disciplines_path


def list_awards_snapshots(root: str = AWARDS_ROOT) -> list:
    """Dates des snapshots d'awards extraits localement (manifest présent), de la plus
    ancienne à la plus récente."""
    return sorted(p.parent.name for p in Path(root).glob("*/awards_manifest.json"))


def awards_dir(date: str = AWARDS_SNAPSHOT, root: str = AWARDS_ROOT) -> Path:
    """Dossier d'un snapshot d'awards extrait ; date None = le plus récent."""
    if date is None:
        dates = list_awards_snapshots(root)
        if not dates:
            raise FileNotFoundError(f"aucun snapshot d'awards dans {root} : lancer extract_awards()")
        date = dates[-1]
    return Path(root) / str(date)


# --- 1. Extraction -------------------------------------------------------------------------

def _sql_awards(url: str, annee_min) -> str:
    """SELECT sur un fichier awards distant ; seuls des sous-champs précis sont cités
    (DuckDB ne lit que ces colonnes du Parquet)."""
    where = f"WHERE start_year >= {int(annee_min)}" if annee_min is not None else ""
    return f"""
    SELECT
        {_short('id')}                                  AS award_id,
        {_short('funder.id')}                           AS funder_id,
        funder_award_id,
        funding_type,
        funder_scheme,
        provenance,
        amount,
        currency,
        start_year::SMALLINT                            AS start_year,
        end_year::SMALLINT                              AS end_year,
        {_short('primary_topic.id')}                    AS topic_id,
        primary_topic.score                             AS topic_score,
        -- institutions bénéficiaires (ROR) et leurs pays, dédoublonnés
        list_filter(list_distinct(list_transform(institution_awarded, i -> {_short('i.id')})),
                    x -> x IS NOT NULL AND x <> '')     AS inst_ids,
        list_filter(list_distinct(list_transform(institution_awarded, i -> i.country_code)),
                    c -> c IS NOT NULL)                 AS inst_countries,
        nullif(regexp_extract(coalesce(lead_investigator.orcid, ''),
                              '(\\d{{4}}-\\d{{4}}-\\d{{4}}-\\d{{3}}[\\dX])', 1), '') AS lead_orcid,
        funded_outputs_count
    FROM read_parquet('{url}')
    {where}
    """


def extract_funders(out_dir: str, source: str, date: str, verbose: bool = True) -> int:
    """Table des financeurs (petite, lue en entière), même snapshot que les awards
    -> <out_dir>/funders.parquet."""
    man = entity_manifest("funders", source=source, date=date)
    urls = "[" + ", ".join(f"'{readable_url(f['url'])}'" for f in man["files"]) + "]"
    con = connect(install=True, daily=source == "daily")
    con.execute(f"""
        CREATE TEMP TABLE f AS
        SELECT {_short('id')} AS funder_id, display_name, country_code,
               {_short('ids.ror')} AS ror, ids.doi AS doi, alternate_titles, awards_count
        FROM read_parquet({urls})
    """)
    n = con.execute("SELECT count(*) FROM f").fetchone()[0]
    con.execute(f"COPY f TO '{Path(out_dir) / 'funders.parquet'}' (FORMAT parquet, COMPRESSION zstd)")
    con.close()
    if verbose:
        print(f"funders ({man.get('source')} {man.get('date')}) : {n:,} financeurs")
    return n


def extract_awards(date: str = AWARDS_SNAPSHOT, source: str = SNAPSHOT_SOURCE,
                   root: str = AWARDS_ROOT, annee_min=AWARDS_ANNEE_MIN, workers: int = 8,
                   test: bool = False, limit: int = None, verbose: bool = True) -> dict:
    """Awards et financeurs d'un snapshot -> <root>/<date>/ (voir l'en-tête).
    date None : snapshot le plus récent de `source` ("daily" : offre payante).
    Reprise : un fichier déjà extrait est sauté. Si annee_min change, tout est relu."""
    date = str(date or latest_snapshot_date(source))
    out_dir = Path(root) / date
    out = out_dir / "extraits"
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("*.tmp"):
        f.unlink()

    # Liste des fichiers : figée au premier lancement (reprise sur le même snapshot)
    p_man = out_dir / "awards_manifest.json"
    if p_man.exists():
        manifest = json.loads(p_man.read_text())
        if manifest.get("source") != source:
            raise RuntimeError(f"{out_dir} contient un snapshot de source {manifest.get('source')!r}")
    else:
        manifest = entity_manifest("awards", source=source, date=date)
        if str(manifest.get("date")) != date:
            raise ValueError(f"snapshot {source} disponible : {manifest.get('date')}, pas {date}")
        p_man.write_text(json.dumps(manifest, indent=1))

    params = {"annee_min": annee_min}
    p_par = out / "params.json"
    if p_par.exists() and json.loads(p_par.read_text()) != params:
        if verbose:
            print("Paramètres d'extraction changés : awards relus")
        for f in out.glob("*.parquet"):
            f.unlink()
    p_par.write_text(json.dumps(params))

    files = manifest["files"]
    if test:
        files = [max(files, key=lambda f: f["meta"].get("record_count") or f["meta"].get("content_length", 0))]
    elif limit:
        files = files[:limit]
    urls = [f["url"] for f in files]
    if verbose:
        faits = sum((out / out_name(u)).exists() for u in urls)
        print(f"Awards {manifest.get('source')} du {manifest.get('date')} : {len(urls)} fichiers, "
              f"{faits} déjà extraits -> {out}")

    def task(url):
        f = out / out_name(url)
        if f.exists():
            return {}
        return extract_file(url, _sql_awards(readable_url(url), annee_min), {f: "SELECT * FROM t"})

    res = run_files(urls, task, workers=workers, verbose=verbose)
    res["financeurs"] = extract_funders(str(out_dir), source, date, verbose)
    res.update(snapshot=date, source=source, dossier=str(out_dir))
    if not test and not limit:
        res.update(check_duplicates(str(out_dir)))
        if verbose and res["doublons"]:
            print(f"ATTENTION : {res['doublons']:,} award_id en double dans {out}")
    return res


def check_duplicates(out_dir: str) -> dict:
    """Chaque award ne doit apparaître qu'une fois."""
    import duckdb
    n, n_distinct = duckdb.connect().execute(f"""
        SELECT count(*), count(DISTINCT award_id) FROM read_parquet('{out_dir}/extraits/*.parquet')
    """).fetchone()
    return {"n_awards": n, "doublons": n - n_distinct}


# --- 2. Pays du financeur x panel ERC ------------------------------------------------------

def _pays_financeur_sql(alias: str = "f") -> str:
    """Pays du financeur : règle commune (outre-mer -> FR), financeurs internationaux à part."""
    expr = pays_sql(f"{alias}.country_code")
    if AWARDS_FINANCEURS_INTERNATIONAUX:
        expr = (f"CASE WHEN {alias}.funder_id IN ({_sql_in(AWARDS_FINANCEURS_INTERNATIONAUX)}) "
                f"THEN '{AWARDS_PAYS_INTERNATIONAL}' ELSE {expr} END")
    return expr


def compute_awards(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, date: str = AWARDS_SNAPSHOT,
                   root: str = AWARDS_ROOT, data_dir: str = DATA_DIR, verbose: bool = True) -> dict:
    """Awards par pays du financeur et panel (voir l'en-tête), pour le snapshot d'awards
    `date` (None : le plus récent extrait). Renvoie les contrôles :
      couverture  DataFrame par pays : nb_awards, part avec topic, part classée, part avec montant
      financeurs  DataFrame : financeurs par pays, volume et part classée
      non_classes topics des awards absents de la nomenclature (devrait être vide)"""
    out_dir = awards_dir(date, root)
    if verbose:
        print(f"Awards du snapshot {out_dir.name} ; nomenclature {nomenclature} de {data_dir}")
    f_disc = disciplines_path(nomenclature, data_dir)
    for f in (f_disc, out_dir / "funders.parquet"):
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : lancer "
                                    + ("build_disciplines (étape 3)" if f == f_disc else "extract_awards()"))
    con = local_connect(data_dir)
    con.execute(f"""
        CREATE TEMP TABLE a AS
        SELECT a.award_id, a.funder_id AS financeur_id, f.display_name AS financeur_nom,
               {_pays_financeur_sql('f')} AS pays_code,
               a.start_year AS annee_debut, a.topic_id,
               d.discipline_code, d.domaine_code, coalesce(d.doute, false) AS doute,
               a.amount AS montant, upper(a.currency) AS devise, a.funding_type
        FROM read_parquet('{out_dir}/extraits/*.parquet') a
        LEFT JOIN read_parquet('{out_dir}/funders.parquet') f ON f.funder_id = a.funder_id
        LEFT JOIN (SELECT * FROM read_parquet('{f_disc}') WHERE discipline_code IS NOT NULL) d
               ON d.topic_id = upper(a.topic_id)
    """)
    f_awards = out_dir / f"awards_{nomenclature}.parquet"
    con.execute(f"COPY a TO '{f_awards}' (FORMAT parquet, COMPRESSION zstd)")

    # Montants : seulement dans la devise principale du pays (la plus fréquente parmi les
    # awards avec montant), les autres devises ne sont pas converties
    f_agg = out_dir / f"awards_pays_{nomenclature}.parquet"
    con.execute(f"""
        COPY (
            WITH dev AS (
                SELECT pays_code, devise FROM (
                    SELECT pays_code, devise, row_number() OVER (
                               PARTITION BY pays_code ORDER BY count(*) DESC, devise) AS r
                    FROM a WHERE montant IS NOT NULL AND devise IS NOT NULL GROUP BY 1, 2)
                WHERE r = 1)
            SELECT a.pays_code, a.annee_debut, a.discipline_code, a.domaine_code,
                   count(*)                                           AS nb_awards,
                   count(a.montant)                                   AS nb_awards_avec_montant,
                   any_value(dev.devise)                              AS devise_principale,
                   sum(a.montant) FILTER (WHERE a.devise = dev.devise) AS montant_devise_principale
            FROM a LEFT JOIN dev USING (pays_code)
            GROUP BY ALL
        ) TO '{f_agg}' (FORMAT parquet, COMPRESSION zstd)
    """)

    res = {
        "couverture": con.execute("""
            SELECT pays_code, count(*) AS nb_awards,
                   round(100 * avg((topic_id IS NOT NULL AND topic_id <> '')::INT), 1) AS pct_avec_topic,
                   round(100 * avg((discipline_code IS NOT NULL)::INT), 1)            AS pct_classes,
                   round(100 * avg((montant IS NOT NULL)::INT), 1)                    AS pct_avec_montant
            FROM a GROUP BY 1 ORDER BY nb_awards DESC
        """).df(),
        "financeurs": con.execute("""
            SELECT pays_code, financeur_id, any_value(financeur_nom) AS financeur_nom,
                   count(*) AS nb_awards,
                   round(100 * avg((discipline_code IS NOT NULL)::INT), 1) AS pct_classes
            FROM a GROUP BY 1, 2 ORDER BY pays_code, nb_awards DESC
        """).df(),
        "non_classes": con.execute("""
            SELECT topic_id, count(*) AS nb_awards FROM a
            WHERE topic_id IS NOT NULL AND topic_id <> '' AND discipline_code IS NULL
            GROUP BY 1 ORDER BY 2 DESC
        """).df(),
        "fichiers": [str(f_awards), str(f_agg)],
        "snapshot_awards": out_dir.name,
    }
    con.close()
    if verbose:
        print(res["couverture"].head(25).to_string(index=False))
        print("\nFinanceurs français :")
        print(res["financeurs"].query("pays_code == 'FR'").head(25).to_string(index=False))
        if len(res["non_classes"]):
            print(f"\nATTENTION : {len(res['non_classes'])} topics absents de la nomenclature {nomenclature}")
    return res
