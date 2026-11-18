"""
projets_scanr.py
================
Base des projets de scanR (projects_denormalized.jsonl.gz : ANR, Horizon Europe, H2020, FP7,
INCa...) enrichie du topic OpenAlex et du panel ERC, pour des indicateurs par panel
(ANR, projets européens...).

Méthode
-------
1. Topic : celui de l'award OpenAlex du même projet (snapshot d'awards extrait par
   extract_awards, AWARDS_SNAPSHOT : peut différer du snapshot des works).
   Appariement id scanR = funder_award_id OpenAlex, après normalisation (PROJETS_GROUPES) :
     ANR      'ANR-21-CE12-0004', 'ANR21CE120004', 'ANR-21-CE12-4' -> 'ANR-21-CE12-0004'
     UE       '101131342', 'H2020-101131342', '0671937'          -> '101131342', '671937'
   et seulement parmi les financeurs OpenAlex du groupe (ROR ou nom de la fiche funder).
   Un projet a souvent plusieurs awards (fiche officielle anr_opendata / cordis + awards
   déclarés dans les articles, sans topic) : on garde l'award avec topic, provenance
   prioritaire (PROJETS_PROVENANCES_PRIORITAIRES), puis meilleur score.
   Les topics OpenAlex déjà présents dans scanR (openalex_primary_topic) ne sont PAS utilisés.
   Projets européens : la référence pour les indicateurs est horizon_topics.py (aboutness sur
   les candidatures et les lauréats Horizon, hors ERC).
2. Panel : topic -> disciplines/<nomenclature>.parquet (étape 3), exactement comme les articles :
   un topic = un panel ; doute / panel alternatif repris de la table.

Sorties dans PROJETS_DIR :
  scanr/projects_denormalized.jsonl.gz      copie brute de scanR (téléchargée si absente)
  projets_scanr_<nomenclature>.parquet   une ligne par projet scanR :
      projet_id, type, groupe, annee, budget_total, budget_finance (participations de scanR, qui font
      foi, à défaut base des projets : controle_scanr.appliquer_montants_scanr), cle_appariement,
      nb_awards_apparies, award_id, award_provenance, topic_id, topic_score, topic_source
      ("award" / NULL), discipline_code, discipline_nom,
      domaine_code, doute, discipline_code_alternative
  projects_denormalized_<nomenclature>.jsonl.gz
      la base scanR complète, chaque projet avec un champ "discipline" :
      {nomenclature, topic_id, topic_source, award_id, discipline_code, discipline_nom, domaine_code, doute}

Exemple :
    from project.server.main.projets_scanr import build_projets_scanr
    res = build_projets_scanr()               # awards du snapshot le plus récent extrait
    res["couverture"]                         # par type scanR : appariés, avec topic, classés
"""

import gzip
import json
from pathlib import Path

import pandas as pd

from .common import _sql_in, connect as local_connect, telecharger
from .config import (AWARDS_ROOT, AWARDS_SNAPSHOT, NOMENCLATURE_DISCIPLINAIRE, DATA_DIR, PROJETS_DIR, PROJETS_GROUPES,
                     PROJETS_PROVENANCES_PRIORITAIRES, PROJETS_SCANR_URL)
from .extract_awards import awards_dir
from .disciplines import disciplines_path


# --- Base scanR ----------------------------------------------------------------------------

def scanr_path(projets_dir: str = PROJETS_DIR, url: str = PROJETS_SCANR_URL) -> Path:
    return Path(projets_dir) / "scanr" / url.rsplit("/", 1)[-1]


def download_scanr(projets_dir: str = PROJETS_DIR, url: str = PROJETS_SCANR_URL,
                   refresh: bool = False, verbose: bool = True) -> Path:
    """Copie locale d'un fichier de scanR (projets, ou participations : PARTICIPATIONS_SCANR_URL),
    re-téléchargée si refresh."""
    return telecharger(url, scanr_path(projets_dir, url), refresh=refresh, verbose=verbose)


def _iter_scanr(f: Path):
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def scanr_table(f: Path) -> pd.DataFrame:
    """Une ligne par projet scanR : les champs utiles à l'appariement et aux indicateurs."""
    rows = [{
        "projet_id": str(p.get("id")),
        "type": p.get("type"),
        "annee": int(p["year"]) if _num(p.get("year")) else None,
        "budget_total": _num(p.get("budgetTotal")),
        "budget_finance": _num(p.get("budgetFinanced")),
    } for p in _iter_scanr(f)]
    return pd.DataFrame(rows)


# --- Normalisation des identifiants (même règle des deux côtés, en SQL) -----------------

def _macros(con) -> None:
    """cle_projet(regle, identifiant) -> clé d'appariement.
    anr    : ANR-AA-CODE-NNNN (numéro sur 4 chiffres, suffixe de partenaire '-01' ignoré) ;
    numero : chiffres seuls, sans zéros en tête ; texte : lettres et chiffres en majuscules."""
    con.execute(r"""
        CREATE OR REPLACE MACRO _anr_tirets(s) AS
            regexp_extract(s, 'ANR-(\d{2})-([A-Z0-9]+)-(\d{1,4})(-|$)', ['a', 'b', 'c']);
        CREATE OR REPLACE MACRO _anr_compact(s) AS
            regexp_extract(replace(s, '-', ''), 'ANR(\d{2})([A-Z][A-Z0-9]*?)(\d{4})$', ['a', 'b', 'c']);
        CREATE OR REPLACE MACRO _cle_anr(s) AS
            CASE WHEN _anr_tirets(s).a <> ''
                 THEN 'ANR-' || _anr_tirets(s).a || '-' || _anr_tirets(s).b || '-' || lpad(_anr_tirets(s).c, 4, '0')
                 WHEN _anr_compact(s).a <> ''
                 THEN 'ANR-' || _anr_compact(s).a || '-' || _anr_compact(s).b || '-' || _anr_compact(s).c
            END;
        CREATE OR REPLACE MACRO cle_projet(regle, x) AS
            CASE regle
                WHEN 'anr' THEN _cle_anr(regexp_replace(upper(x), '[^A-Z0-9]+', '-', 'g'))
                WHEN 'numero' THEN nullif(ltrim(regexp_extract(x, '(\d{5,9})', 1), '0'), '')
                ELSE nullif(regexp_replace(upper(x), '[^A-Z0-9]', '', 'g'), '')
            END;
    """)


def _groupes_sql() -> tuple:
    """(table des types scanR -> groupe, condition SQL financeur -> groupe)."""
    types = pd.DataFrame([{"type": t, "groupe": g, "regle": c["cle"]}
                          for g, c in PROJETS_GROUPES.items() for t in c["types"]])
    cas = []
    for g, c in PROJETS_GROUPES.items():
        cond = []
        if c.get("ror"):
            cond.append(f"f.ror IN ({_sql_in(c['ror'])})")
        if c.get("nom_regex"):
            cond.append(f"regexp_matches(f.display_name, '{c['nom_regex'].replace(chr(39), chr(39) * 2)}')")
        if cond:
            cas.append(f"WHEN {' OR '.join(cond)} THEN '{g}'")
    return types, f"CASE {' '.join(cas)} END" if cas else "NULL"


# --- Construction ---------------------------------------------------------------------------

def build_projets_scanr(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, awards_date: str = AWARDS_SNAPSHOT,
                        awards_root: str = AWARDS_ROOT, data_dir: str = DATA_DIR,
                        projets_dir: str = PROJETS_DIR, refresh_scanr: bool = False,
                        enrichi: bool = True,
                        verbose: bool = True) -> dict:
    """Projets scanR + topic OpenAlex + panel ERC (voir l'en-tête). Renvoie les contrôles :
      couverture   DataFrame par type scanR : nb_projets, % appariés à un award, % avec topic
                   (award / scanR), % classés dans un panel
      financeurs   financeurs OpenAlex retenus par groupe (contrôle des ROR / noms)"""
    projets_dir = Path(projets_dir)
    a_dir = awards_dir(awards_date, awards_root)
    f_disc = disciplines_path(nomenclature, data_dir)
    for f in (f_disc, a_dir / "funders.parquet"):
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : lancer "
                                    + ("build_disciplines (étape 3)" if f == f_disc else "extract_awards()"))
    f_scanr = download_scanr(projets_dir, refresh=refresh_scanr, verbose=verbose)
    if verbose:
        print(f"scanR : {f_scanr} ; awards du snapshot {a_dir.name} ; nomenclature {nomenclature} de {data_dir}")

    con = local_connect(data_dir)
    _macros(con)
    types, cas_financeur = _groupes_sql()
    con.register("types_df", types)
    # budgets : participations de scanR (font foi), à défaut base des projets
    from .controle_scanr import appliquer_montants_scanr
    scanr_df, _, controle_montants = appliquer_montants_scanr(scanr_table(f_scanr), projets_dir=projets_dir,
                                                              verbose=verbose)
    con.register("scanr_df", scanr_df.drop(columns="source_montant"))

    # Financeurs OpenAlex de chaque groupe
    con.execute(f"""
        CREATE TEMP TABLE fin AS
        SELECT funder_id, display_name, ror, {cas_financeur} AS groupe
        FROM read_parquet('{a_dir}/funders.parquet') f
    """)
    con.execute("DELETE FROM fin WHERE groupe IS NULL")
    # Awards des financeurs retenus, avec la clé d'appariement de leur groupe
    con.execute(f"""
        CREATE TEMP TABLE aw AS
        SELECT a.award_id, a.provenance, a.topic_id, a.topic_score, fin.groupe,
               cle_projet(g.regle, a.funder_award_id) AS cle
        FROM read_parquet('{a_dir}/extraits/*.parquet') a
        JOIN fin USING (funder_id)
        JOIN (SELECT DISTINCT groupe, regle FROM types_df) g USING (groupe)
    """)
    con.execute("DELETE FROM aw WHERE cle IS NULL")
    # Projets scanR et clé
    con.execute("""
        CREATE TEMP TABLE p AS
        SELECT s.*, t.groupe, cle_projet(t.regle, s.projet_id) AS cle_appariement
        FROM scanr_df s LEFT JOIN types_df t USING (type)
    """)
    # Meilleur award par projet : avec topic, provenance prioritaire, meilleur score
    prio = PROJETS_PROVENANCES_PRIORITAIRES or ["--"]
    con.execute(f"""
        CREATE TEMP TABLE m AS
        SELECT projet_id, type, nb_awards_apparies, award_id, award_provenance,
               CASE WHEN topic_id <> '' THEN upper(topic_id) END AS topic_award, topic_score
        FROM (
            SELECT p.projet_id, p.type, aw.award_id, aw.provenance AS award_provenance, aw.topic_id, aw.topic_score,
                   count(*) OVER (PARTITION BY p.projet_id, p.type) AS nb_awards_apparies,
                   row_number() OVER (PARTITION BY p.projet_id, p.type ORDER BY
                       (aw.topic_id IS NOT NULL AND aw.topic_id <> '') DESC,
                       list_position([{_sql_in(prio)}], aw.provenance) IS NOT NULL DESC,
                       coalesce(list_position([{_sql_in(prio)}], aw.provenance), 99),
                       aw.topic_score DESC NULLS LAST, aw.award_id) AS r
            FROM p JOIN aw ON aw.groupe = p.groupe AND aw.cle = p.cle_appariement
        ) WHERE r = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE res AS
        WITH t AS (
            SELECT p.projet_id, p.type, p.groupe, p.annee, p.budget_total, p.budget_finance,
                   p.cle_appariement, coalesce(m.nb_awards_apparies, 0) AS nb_awards_apparies,
                   m.award_id, m.award_provenance, m.topic_score,
                   m.topic_award AS topic_id,
                   CASE WHEN m.topic_award IS NOT NULL THEN 'award' END AS topic_source
            FROM p LEFT JOIN m USING (projet_id, type)
        ), d AS (SELECT * FROM read_parquet('{f_disc}') WHERE discipline_code IS NOT NULL)
        SELECT t.projet_id, t.type, t.groupe, t.annee, t.budget_total, t.budget_finance,
               t.cle_appariement, t.nb_awards_apparies, t.award_id, t.award_provenance,
               t.topic_id, t.topic_score, t.topic_source,
               d.discipline_code, d.discipline_nom, d.domaine_code, coalesce(d.doute, false) AS doute,
               d.discipline_code_alternative
        FROM t LEFT JOIN d ON d.topic_id = t.topic_id
    """)
    projets_dir.mkdir(parents=True, exist_ok=True)
    f_out = projets_path(nomenclature, projets_dir)
    con.execute(f"COPY res TO '{f_out}' (FORMAT parquet, COMPRESSION zstd)")

    res = {
        "snapshot_awards": a_dir.name,
        "montants_scanr": controle_montants,
        "fichiers": [str(f_out)],
        "couverture": con.execute("""
            SELECT type, any_value(groupe) AS groupe, count(*) AS nb_projets,
                   round(100 * avg((nb_awards_apparies > 0)::INT), 1)          AS pct_apparies,
                   round(100 * avg(coalesce(topic_source = 'award', false)::INT), 1)          AS pct_topic_award,
                   round(100 * avg((discipline_code IS NOT NULL)::INT), 1)     AS pct_classes
            FROM res GROUP BY type ORDER BY nb_projets DESC
        """).df(),
        "financeurs": con.execute("""
            SELECT groupe, funder_id, display_name, ror FROM fin ORDER BY groupe, display_name
        """).df(),
        "non_classes": con.execute("""
            SELECT topic_id, count(*) AS n FROM res
            WHERE topic_id IS NOT NULL AND discipline_code IS NULL GROUP BY 1 ORDER BY 2 DESC
        """).df(),
    }
    enr = con.execute("""
        SELECT projet_id, type, topic_id, topic_source, award_id, discipline_code, discipline_nom,
               domaine_code, doute FROM res
    """).df()
    con.close()

    if enrichi:
        res["fichiers"].append(str(write_enrichi(f_scanr, enr, enrichi_path(nomenclature, projets_dir), nomenclature)))
    if verbose:
        print(res["couverture"].to_string(index=False))
        print("\nFinanceurs OpenAlex retenus par groupe :", res["financeurs"].groupby("groupe").size().to_dict())
        if len(res["non_classes"]):
            print(f"ATTENTION : {len(res['non_classes'])} topics absents de la nomenclature {nomenclature}")
    return res


def projets_path(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, projets_dir: str = PROJETS_DIR) -> Path:
    """Une ligne par projet scanR avec topic et panel (lue aussi par anr.py)."""
    return Path(projets_dir) / f"projets_scanr_{nomenclature}.parquet"


def enrichi_path(nomenclature: str = NOMENCLATURE_DISCIPLINAIRE, projets_dir: str = PROJETS_DIR) -> Path:
    """Base scanR complète enrichie (lue aussi par projects.generate_projects)."""
    return Path(projets_dir) / f"projects_denormalized_{nomenclature}.jsonl.gz"


def write_enrichi(f_scanr: Path, enr: pd.DataFrame, f_out: Path, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE) -> Path:
    """Réécrit la base scanR complète avec, pour chaque projet, le champ discipline."""
    enr = enr.astype(object).where(enr.notna(), None)
    enr.insert(0, "nomenclature_disciplinaire", nomenclature)
    par_id = {(r.pop("projet_id"), r.pop("type")): r for r in enr.to_dict("records")}
    tmp = f_out.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        for p in _iter_scanr(f_scanr):
            p["discipline"] = par_id.get((str(p.get("id")), p.get("type")))
            out.write(json.dumps(p, ensure_ascii=False) + "\n")
    tmp.rename(f_out)
    return f_out
