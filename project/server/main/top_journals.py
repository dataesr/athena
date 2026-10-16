"""
top_journals.py
===============
Étape 5 : les revues de tête de chaque discipline (indicateur de prestige).

Méthode
-------
1. Lecture du fichier SCImago (SJR) : séparateur « ; », décimales à virgule,
   ISSN sans tiret séparés par des virgules. On garde les types SJR_TYPES.
2. Appariement SJR <-> sources OpenAlex par ISSN (tous les ISSN des deux
   côtés). Une revue = un identifiant SJR ; plusieurs sources OpenAlex en
   doublon peuvent pointer vers la même revue SJR (leurs articles sont
   regroupés). Si une source OpenAlex correspond à plusieurs revues SJR,
   on garde celle au SJR le plus élevé.
3. Rattachement revue -> discipline, sur les articles du périmètre
   (config.py) des années JOURNAL_REF_YEARS :
     n_d  = articles de la revue classés dans la discipline d
     part = n_d / articles de la revue
   Une revue est éligible pour d si n_d >= JOURNAL_MIN_ARTICLES,
   part >= JOURNAL_MIN_SHARE, et si ce n'est pas une revue de synthèse
   (part de « review » > JOURNAL_MAX_REVIEW_SHARE, titre reconnu par
   JOURNAL_REVIEW_TITLE_REGEX : Nature Reviews, Annual Review of, Trends in,
   ... Surveys, ou plus de JOURNAL_MAX_REFS_PER_DOC références par document
   selon SJR, critère optionnel).
   Le monde choisi (WORLD) ne change rien ici : seules les revues SJR comptent.
4. Revues de tête de d = les TOP_JOURNALS_N revues éligibles au SJR le plus
   élevé (à égalité : la plus grosse en n_d).
   À l'étape 6, seuls les articles de d parus dans une revue de tête de d
   comptent pour d : une revue généraliste ne « gonfle » pas les autres
   disciplines.
5. Option « revues multidisciplinaires » (MULTIDISC_TOP_N > 0) : voir ci-dessous.

Revues multidisciplinaires (option)
-----------------------------------
Pourquoi : les très grandes revues généralistes (Nature, Science, PNAS...)
publient dans toutes les disciplines ; aucune discipline n'y dépasse
JOURNAL_MIN_SHARE, donc la règle du point 3 ne les retient jamais. Or y
publier est sans doute le signal de prestige le plus partagé entre disciplines.

Règle : on ajoute aux revues de tête de CHAQUE discipline les MULTIDISC_TOP_N
revues multidisciplinaires au SJR le plus élevé. Une revue est
« multidisciplinaire » si sa liste de catégories SJR contient
MULTIDISC_CATEGORY (« Multidisciplinary »). Elle doit en plus :
  - être appariée à OpenAlex et compter au moins MULTIDISC_MIN_ARTICLES
    articles du périmètre (toutes disciplines confondues) : une vraie revue
    généraliste publie des milliers d'articles ; le seuil écarte les revues
    jeunes ou confidentielles classées « Multidisciplinary » dont le SJR est
    élevé mais instable (ex. Exploration, 354 articles) ;
  - ne pas être une revue de synthèse (mêmes critères qu'au point 3).
Le seuil MULTIDISC_TOP_N est indispensable : PLoS ONE ou Scientific Reports
sont aussi classées « Multidisciplinary » mais ne sont pas des revues de tête.
On peut aussi imposer la liste à la main avec MULTIDISC_SJR_IDS (identifiants
SJR) : seules ces revues sont alors retenues, sans classement.

Comptage (étape 6) : inchangé. Un article de Nature classé en PE10 compte
seulement pour PE10 ; il n'y a ni double compte entre disciplines, ni
« gonflement » d'une discipline par les autres.

Conséquences à garder en tête :
  - une discipline a désormais TOP_JOURNALS_N revues propres + MULTIDISC_TOP_N
    revues communes (10 + 5 par défaut) ;
  - le poids des revues multidisciplinaires varie selon les disciplines
    (fort en biologie ou en physique, faible en sciences humaines) : la
    colonne `categorie` de la liste permet de calculer le prestige avec ou
    sans elles à l'étape 6, pour mesurer leur effet.

Relecture humaine
-----------------
Le fichier <nom>_revue.csv liste, par discipline, les meilleures revues
candidates (éligibles ou non, avec le motif d'exclusion), et une colonne
`retenue` (1 = dans la liste). Le fichier <nom>_multidisciplinaires.csv fait
de même pour les revues multidisciplinaires. On peut modifier `retenue`
dans un tableur puis appeler top_journals_from_review() pour en faire la
liste officielle.

Sorties (dans <data_dir>/top_journals/)
---------------------------------------
  sjr_sources.parquet   source_id (OpenAlex) -> sjr_id, titre, sjr, quartile
  <nom>.parquet         discipline_code, categorie, rang, sjr_id, titre, sjr, quartile,
                        n_articles_discipline, part_discipline   <- utilisé par l'étape 6
                        categorie = 'disciplinaire' (rang 1..TOP_JOURNALS_N)
                                  | 'multidisciplinaire' (rang 1..MULTIDISC_TOP_N,
                                    même liste répétée pour chaque discipline)
  <nom>_revue.csv       candidates par discipline, pour relecture
  <nom>_multidisciplinaires.csv  candidates multidisciplinaires, pour relecture
  <nom>_params.json     paramètres utilisés

Exemples :
    from project.server.main.top_journals import compute_top_journals, top_journals_from_review
    res = compute_top_journals()
    res = compute_top_journals(min_share=0.10, out_name="erc_part10")
    # après relecture / modification du CSV :
    top_journals_from_review("/data/top_journals/erc_revue.csv", out_name="erc")
"""

import json
from pathlib import Path

import pandas as pd

from .common import connect, perimeter_where
from .config import (ANNEE_MAX, ANNEE_MIN, DATA_DIR, DISCIPLINES_NAME,
                     JOURNAL_MAX_REFS_PER_DOC, JOURNAL_MAX_REVIEW_SHARE,
                     JOURNAL_MIN_ARTICLES, JOURNAL_MIN_SHARE,
                     JOURNAL_REF_YEARS, JOURNAL_REVIEW_TITLE_REGEX,
                     MULTIDISC_CATEGORY, MULTIDISC_MIN_ARTICLES, MULTIDISC_SJR_IDS,
                     MULTIDISC_TOP_N,
                     SJR_PATH, SJR_TYPES, SOURCE_TYPES, TOP_JOURNALS_N, WORK_TYPES)


def load_sjr(sjr_path: str = SJR_PATH, sjr_types=SJR_TYPES) -> tuple:
    """Lit le CSV SCImago. Renvoie (revues, issn) :
      revues : sjr_id, titre, type, sjr, quartile, categories, areas
      issn   : sjr_id, issn (une ligne par ISSN, 8 caractères, sans tiret)"""
    cols = {"Sourceid": "sjr_id", "Title": "titre", "Type": "type", "Issn": "issn",
            "SJR": "sjr", "SJR Best Quartile": "quartile",
            "Ref. / Doc.": "refs_par_doc",
            "Categories": "categories", "Areas": "areas"}
    df = pd.read_csv(sjr_path, sep=";", dtype=str, encoding="utf-8-sig",
                     usecols=lambda c: c in cols)
    df = df.rename(columns=cols)
    df["sjr_id"] = df["sjr_id"].astype("int64")
    for c in ("sjr", "refs_par_doc"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="coerce")
        else:
            df[c] = float("nan")
    if sjr_types:
        df = df[df["type"].isin(sjr_types)]

    issn = (df[["sjr_id", "issn"]].dropna()
            .assign(issn=lambda d: d["issn"].str.split(","))
            .explode("issn"))
    issn["issn"] = issn["issn"].str.strip().str.replace("-", "", regex=False).str.upper()
    issn = issn[issn["issn"].str.len() == 8].drop_duplicates()
    return df.drop(columns="issn").reset_index(drop=True), issn.reset_index(drop=True)


def build_sjr_sources(con, sjr_path: str = SJR_PATH, sjr_types=SJR_TYPES,
                      data_dir: str = DATA_DIR) -> dict:
    """Appariement sources OpenAlex <-> SJR -> <data_dir>/top_journals/sjr_sources.parquet.
    Crée aussi les tables `sjr` et `sjr_sources` dans la connexion."""
    revues, issn = load_sjr(sjr_path, sjr_types)
    con.register("sjr_df", revues)
    con.register("sjr_issn_df", issn)
    con.execute("CREATE OR REPLACE TEMP TABLE sjr AS SELECT * FROM sjr_df")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sjr_sources AS
        WITH src AS (
            SELECT source_id, unnest(issn_norm) AS issn
            FROM read_parquet('{data_dir}/sources.parquet')
        ), m AS (
            SELECT DISTINCT src.source_id, i.sjr_id
            FROM src JOIN sjr_issn_df i USING (issn)
        )
        SELECT m.source_id,
               arg_max(s.sjr_id, coalesce(s.sjr, -1)) AS sjr_id
        FROM m JOIN sjr s USING (sjr_id)
        GROUP BY m.source_id
    """)
    out = Path(data_dir) / "top_journals" / "sjr_sources.parquet"
    con.execute(f"""
        COPY (SELECT ss.source_id, s.sjr_id, s.titre, s.sjr, s.quartile
              FROM sjr_sources ss JOIN sjr s USING (sjr_id))
        TO '{out}' (FORMAT parquet)
    """)
    n_sjr = len(revues)
    n_match = con.execute("SELECT count(DISTINCT sjr_id) FROM sjr_sources").fetchone()[0]
    n_src = con.execute("SELECT count(*) FROM sjr_sources").fetchone()[0]
    return {"n_revues_sjr": n_sjr, "n_revues_sjr_appariees": n_match,
            "pct_revues_sjr_appariees": round(100 * n_match / n_sjr, 1) if n_sjr else None,
            "n_sources_openalex_appariees": n_src}


def compute_top_journals(disciplines: str = DISCIPLINES_NAME,
                         sjr_path: str = SJR_PATH,
                         data_dir: str = DATA_DIR,
                         n_top: int = TOP_JOURNALS_N,
                         min_articles: int = JOURNAL_MIN_ARTICLES,
                         min_share: float = JOURNAL_MIN_SHARE,
                         sjr_types=SJR_TYPES,
                         ref_years=JOURNAL_REF_YEARS,
                         max_review_share: float = JOURNAL_MAX_REVIEW_SHARE,
                         review_title_regex: str = JOURNAL_REVIEW_TITLE_REGEX,
                         max_refs_per_doc: float = JOURNAL_MAX_REFS_PER_DOC,
                         multidisc_top_n: int = MULTIDISC_TOP_N,
                         multidisc_category: str = MULTIDISC_CATEGORY,
                         multidisc_min_articles: int = MULTIDISC_MIN_ARTICLES,
                         multidisc_sjr_ids=MULTIDISC_SJR_IDS,
                         work_types=WORK_TYPES,
                         source_types=SOURCE_TYPES,
                         annee_min: int = ANNEE_MIN,
                         annee_max: int = ANNEE_MAX,
                         n_review: int = 30,
                         out_name: str = None,
                         verbose: bool = True) -> dict:
    """Revues de tête par discipline (voir l'en-tête du module).

    n_review : nombre de candidates par discipline dans le CSV de relecture.
    max_review_share / review_title_regex : exclusion des revues de synthèse
    (None pour désactiver l'un ou l'autre critère).
    multidisc_top_n / multidisc_category / multidisc_sjr_ids : option des
    revues multidisciplinaires (0 ou None pour la désactiver).
    Renvoie les contrôles d'appariement, 'par_annee' (part des articles du
    périmètre parus dans une revue SJR), 'disciplines_incompletes' et 'top'.
    """
    data_dir = Path(data_dir)
    out_name = out_name or disciplines
    out_dir = data_dir / "top_journals"
    out_dir.mkdir(parents=True, exist_ok=True)
    con = connect(data_dir)
    res = build_sjr_sources(con, sjr_path, sjr_types, data_dir)

    y0, y1 = ref_years if ref_years else (annee_min, annee_max)
    perim = perimeter_where(work_types, source_types, annee_min, annee_max, "w")
    ref = perimeter_where(None, None, y0, y1, "w")

    # Articles du périmètre avec discipline et revue SJR éventuelle
    con.execute(f"""
        CREATE TEMP TABLE art AS
        SELECT w.year, d.discipline_code, ss.sjr_id
        FROM read_parquet('{data_dir}/works/*.parquet') w
        JOIN read_parquet('{data_dir}/disciplines/{disciplines}.parquet') d
          ON upper(w.topic_id) = d.topic_id
        LEFT JOIN sjr_sources ss ON ss.source_id = w.source_id
        WHERE d.discipline_code IS NOT NULL AND {perim}
    """)

    # Part de « review » par revue (tous types extraits, années de référence)
    con.execute(f"""
        CREATE TEMP TABLE types_revue AS
        SELECT ss.sjr_id, avg((w.type = 'review')::INT) AS part_reviews
        FROM read_parquet('{data_dir}/works/*.parquet') w
        JOIN sjr_sources ss ON ss.source_id = w.source_id
        WHERE {ref}
        GROUP BY 1
    """)

    # Candidates : revue x discipline, sur les années de référence
    con.execute(f"""
        CREATE TEMP TABLE cand AS
        WITH c AS (
            SELECT sjr_id, discipline_code, count(*) AS n_d
            FROM art w WHERE sjr_id IS NOT NULL AND {ref}
            GROUP BY ALL
        )
        SELECT c.*, (sum(n_d) OVER (PARTITION BY sjr_id))::BIGINT AS n_revue,
               n_d / sum(n_d) OVER (PARTITION BY sjr_id) AS part,
               s.titre, s.sjr, s.quartile, s.refs_par_doc,
               coalesce(tr.part_reviews, 0) AS part_reviews
        FROM c JOIN sjr s USING (sjr_id)
        LEFT JOIN types_revue tr USING (sjr_id)
    """)
    synth_parts = []
    if max_review_share is not None:
        synth_parts.append(
            (f"part_reviews > {float(max_review_share)}",
             f"'reviews > {round(100 * max_review_share)} %'"))
    if max_refs_per_doc is not None:
        synth_parts.append(
            (f"coalesce(refs_par_doc, 0) > {float(max_refs_per_doc)}",
             f"'> {max_refs_per_doc:g} réf./doc'"))
    if review_title_regex:
        rx = review_title_regex.replace("'", "''")
        synth_parts.append((f"regexp_matches(titre, '{rx}')", "'titre de revue de synthèse'"))
    synthese = " OR ".join(c for c, _ in synth_parts) or "FALSE"
    motifs = ", ".join(
        [f"CASE WHEN part < {float(min_share)} THEN 'part < {round(100 * min_share)} %' END"]
        + [f"CASE WHEN {c} THEN {m} END" for c, m in synth_parts])
    con.execute(f"""
        CREATE TEMP TABLE classement AS
        SELECT *,
               (n_d >= {int(min_articles)} AND part >= {float(min_share)}
                AND NOT ({synthese})) AS eligible,
               nullif(concat_ws(' ; ', {motifs}), '') AS motif_exclusion,
               row_number() OVER (PARTITION BY discipline_code
                                  ORDER BY sjr DESC NULLS LAST, n_d DESC) AS rang_sjr
        FROM cand
        WHERE n_d >= {int(min_articles)}
    """)
    con.execute(f"""
        CREATE TEMP TABLE top AS
        SELECT discipline_code, 'disciplinaire' AS categorie,
               row_number() OVER (PARTITION BY discipline_code
                                  ORDER BY sjr DESC NULLS LAST, n_d DESC) AS rang,
               sjr_id, titre, sjr, quartile,
               n_d AS n_articles_discipline, round(part, 4) AS part_discipline
        FROM classement WHERE eligible
        QUALIFY rang <= {int(n_top)}
    """)

    # --- Revues multidisciplinaires (option) ------------------------------------
    multi_on = bool(multidisc_top_n) or bool(multidisc_sjr_ids)
    multi = pd.DataFrame()
    if multi_on:
        if multidisc_sjr_ids:
            selection = f"sjr_id IN ({', '.join(str(int(i)) for i in multidisc_sjr_ids)})"
            limite = len(multidisc_sjr_ids)
        else:
            cat = multidisc_category.replace("'", "''")
            selection = f"contains(coalesce(categories, ''), '{cat}')"
            limite = int(multidisc_top_n)
        con.execute(f"""
            CREATE TEMP TABLE multi_cand AS
            WITH nr AS (
                SELECT sjr_id, count(*) AS n_revue
                FROM art w WHERE sjr_id IS NOT NULL AND {ref} GROUP BY 1
            ), base AS (
                SELECT s.sjr_id, s.titre, s.sjr, s.quartile, s.categories, s.refs_par_doc,
                       coalesce(nr.n_revue, 0) AS n_revue,
                       coalesce(tr.part_reviews, 0) AS part_reviews
                FROM sjr s
                LEFT JOIN nr USING (sjr_id)
                LEFT JOIN types_revue tr USING (sjr_id)
                WHERE {selection}
            )
            SELECT *,
                   (n_revue >= {int(multidisc_min_articles)} AND NOT ({synthese})) AS eligible,
                   nullif(concat_ws(' ; ',
                       CASE WHEN n_revue = 0 THEN 'absente d''OpenAlex / du périmètre'
                            WHEN n_revue < {int(multidisc_min_articles)} THEN 'moins de {int(multidisc_min_articles)} articles'
                       END, {", ".join(f"CASE WHEN {c} THEN {m} END" for c, m in synth_parts) or "NULL"}
                   ), '') AS motif_exclusion,
                   row_number() OVER (ORDER BY sjr DESC NULLS LAST) AS rang_sjr
            FROM base
        """)
        con.execute(f"""
            CREATE TEMP TABLE multi AS
            SELECT row_number() OVER (ORDER BY sjr DESC NULLS LAST) AS rang, *
            FROM multi_cand WHERE eligible
            QUALIFY rang <= {limite}
        """)
        # même liste ajoutée à chaque discipline (sauf si la revue y est déjà
        # parmi les revues disciplinaires)
        con.execute("""
            INSERT INTO top
            SELECT d.discipline_code, 'multidisciplinaire', m.rang, m.sjr_id, m.titre,
                   m.sjr, m.quartile, coalesce(c.n_d, 0), round(coalesce(c.n_d, 0) / m.n_revue, 4)
            FROM (SELECT DISTINCT discipline_code FROM art) d
            CROSS JOIN multi m
            LEFT JOIN cand c ON c.sjr_id = m.sjr_id AND c.discipline_code = d.discipline_code
            WHERE NOT EXISTS (SELECT 1 FROM top t
                              WHERE t.discipline_code = d.discipline_code AND t.sjr_id = m.sjr_id)
        """)
        multi = con.execute("""
            SELECT c.rang_sjr, c.sjr_id, c.titre, c.sjr, c.quartile, c.categories,
                   c.n_revue AS n_articles_revue, round(c.part_reviews, 3) AS part_reviews,
                   c.refs_par_doc,
                   c.eligible::INT AS eligible, coalesce(c.motif_exclusion, '') AS motif_exclusion,
                   (m.sjr_id IS NOT NULL)::INT AS retenue
            FROM multi_cand c LEFT JOIN multi m USING (sjr_id)
            WHERE c.rang_sjr <= 30 OR m.sjr_id IS NOT NULL
            ORDER BY c.rang_sjr
        """).df()
        multi.to_csv(out_dir / f"{out_name}_multidisciplinaires.csv", sep=";", decimal=",",
                     index=False, encoding="utf-8-sig")

    con.execute(f"COPY (SELECT * FROM top ORDER BY discipline_code, categorie, rang) "
                f"TO '{out_dir}/{out_name}.parquet' (FORMAT parquet)")

    # CSV de relecture : les n_review premières candidates par discipline
    revue = con.execute(f"""
        SELECT c.discipline_code, n.discipline_name, c.rang_sjr, c.sjr_id, c.titre,
               c.sjr, c.quartile, c.n_d AS n_articles_discipline,
               c.n_revue AS n_articles_revue, round(c.part, 4) AS part_discipline,
               round(c.part_reviews, 3) AS part_reviews, c.refs_par_doc,
               c.eligible::INT AS eligible, coalesce(c.motif_exclusion, '') AS motif_exclusion,
               (t.sjr_id IS NOT NULL)::INT AS retenue
        FROM classement c
        LEFT JOIN (SELECT * FROM top WHERE categorie = 'disciplinaire') t
               USING (discipline_code, sjr_id)
        LEFT JOIN (SELECT DISTINCT discipline_code, discipline_name
                   FROM read_parquet('{data_dir}/disciplines/{disciplines}.parquet')) n
               USING (discipline_code)
        WHERE c.rang_sjr <= {int(n_review)}
        ORDER BY c.discipline_code, c.rang_sjr
    """).df()
    f_revue = out_dir / f"{out_name}_revue.csv"
    revue.to_csv(f_revue, sep=";", decimal=",", index=False, encoding="utf-8-sig")

    params = dict(disciplines=disciplines, sjr_path=str(sjr_path), n_top=n_top,
                  min_articles=min_articles, min_share=min_share,
                  sjr_types=list(sjr_types or []), ref_years=[y0, y1],
                  max_review_share=max_review_share, review_title_regex=review_title_regex,
                  max_refs_per_doc=max_refs_per_doc, multidisc_min_articles=multidisc_min_articles,
                  multidisc_top_n=multidisc_top_n, multidisc_category=multidisc_category,
                  multidisc_sjr_ids=list(multidisc_sjr_ids or []),
                  multidisc_retenues=(multi.loc[multi["retenue"] == 1, "titre"].tolist()
                                      if len(multi) else []),
                  work_types=list(work_types or []), source_types=list(source_types or []),
                  annee_min=annee_min, annee_max=annee_max)
    (out_dir / f"{out_name}_params.json").write_text(json.dumps(params, indent=1))

    # Contrôles
    res["par_annee"] = con.execute("""
        SELECT year, count(*) AS n,
               round(100 * avg((sjr_id IS NOT NULL)::INT), 1) AS pct_revue_sjr
        FROM art GROUP BY year ORDER BY year
    """).df()
    res["disciplines_incompletes"] = con.execute(f"""
        SELECT n.discipline_code, coalesce(t.n, 0) AS n_revues_tete
        FROM (SELECT DISTINCT discipline_code FROM art) n
        LEFT JOIN (SELECT discipline_code, count(*) AS n FROM top
                   WHERE categorie = 'disciplinaire' GROUP BY 1) t USING (discipline_code)
        WHERE coalesce(t.n, 0) < {int(n_top)} ORDER BY 1
    """).df()
    res["top"] = con.execute("SELECT * FROM top ORDER BY discipline_code, categorie, rang").df()
    res["multidisciplinaires"] = multi
    res["fichier"] = str(out_dir / f"{out_name}.parquet")
    res["fichier_revue"] = str(f_revue)
    con.close()

    if verbose:
        print(f"SJR : {res['n_revues_sjr']:,} revues, {res['n_revues_sjr_appariees']:,} appariées "
              f"({res['pct_revues_sjr_appariees']} %) à {res['n_sources_openalex_appariees']:,} "
              f"sources OpenAlex")
        print("\nPart des articles du périmètre parus dans une revue SJR :")
        print(res["par_annee"].to_string(index=False))
        if len(res["disciplines_incompletes"]):
            print(f"\nDisciplines avec moins de {n_top} revues de tête :")
            print(res["disciplines_incompletes"].to_string(index=False))
        if len(multi):
            print(f"\nRevues multidisciplinaires ajoutées à toutes les disciplines :")
            print(multi[multi["retenue"] == 1][["titre", "sjr", "n_articles_revue"]]
                  .to_string(index=False))
        print(f"\nTop 3 disciplinaire par discipline (liste complète : {res['fichier_revue']}) :")
        t = res["top"]
        t = t[(t["categorie"] == "disciplinaire") & (t["rang"] <= 3)]
        print(t[["discipline_code", "rang", "titre", "sjr",
                 "n_articles_discipline", "part_discipline"]].to_string(index=False))
    return res


def _retenues(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["retenue"].astype(str).str.strip().isin(["1", "1.0", "oui", "True"])]


def top_journals_from_review(review_csv: str, out_name: str = None,
                             multidisc_csv: str = "auto",
                             data_dir: str = DATA_DIR) -> pd.DataFrame:
    """Fait de la liste relue la liste officielle <data_dir>/top_journals/<out_name>.parquet
    utilisée par l'étape 6.

    review_csv    : <nom>_revue.csv relu ; lignes `retenue` = 1 -> revues disciplinaires
    multidisc_csv : <nom>_multidisciplinaires.csv relu ; ses lignes `retenue` = 1 sont
                    ajoutées à toutes les disciplines. "auto" = le fichier voisin s'il
                    existe ; None = pas de revues multidisciplinaires.
    out_name      : défaut = nom du CSV sans le suffixe _revue.
    Après relecture, n_articles_discipline / part_discipline des revues
    multidisciplinaires ne sont pas recalculés (laissés vides)."""
    review_csv = Path(review_csv)
    base = review_csv.stem.removesuffix("_revue")
    out_name = out_name or base
    lire = lambda p: pd.read_csv(p, sep=";", decimal=",", encoding="utf-8-sig")

    tout = lire(review_csv)
    df = _retenues(tout).sort_values(["discipline_code", "sjr"], ascending=[True, False])
    df["rang"] = df.groupby("discipline_code").cumcount() + 1
    df["categorie"] = "disciplinaire"
    cols = ["discipline_code", "categorie", "rang", "sjr_id", "titre", "sjr", "quartile",
            "n_articles_discipline", "part_discipline"]
    top = df[cols].astype({"n_articles_discipline": "Int64", "part_discipline": "Float64"})

    if multidisc_csv == "auto":
        p = review_csv.with_name(f"{base}_multidisciplinaires.csv")
        multidisc_csv = p if p.exists() else None
    if multidisc_csv:
        m = _retenues(lire(multidisc_csv)).sort_values("sjr", ascending=False)
        m = m.assign(rang=range(1, len(m) + 1), categorie="multidisciplinaire",
                     n_articles_discipline=pd.array([pd.NA] * len(m), dtype="Int64"),
                     part_discipline=pd.array([pd.NA] * len(m), dtype="Float64"))
        disc = pd.DataFrame({"discipline_code": sorted(tout["discipline_code"].unique())})
        m = disc.merge(m, how="cross")
        m = m.merge(top[["discipline_code", "sjr_id"]], how="left", indicator=True,
                    on=["discipline_code", "sjr_id"])
        m = m[m["_merge"] == "left_only"]
        top = pd.concat([top, m[cols]], ignore_index=True)

    top = top.sort_values(["discipline_code", "categorie", "rang"]).reset_index(drop=True)
    top["n_articles_discipline"] = top["n_articles_discipline"].astype("Int64")
    top["part_discipline"] = top["part_discipline"].astype("Float64")
    con = connect(data_dir)
    con.register("top_df", top)
    con.execute(f"COPY top_df TO '{Path(data_dir) / 'top_journals' / out_name}.parquet' "
                f"(FORMAT parquet)")
    con.close()
    return top
