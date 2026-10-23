"""
disciplines.py
==============
Étape 3 : classer chaque topic OpenAlex dans une discipline.

Ce n'est qu'un classement : aucun filtre de périmètre (type de document,
type de source...) n'est appliqué ici. Le contrôle de couverture porte sur
tout ce qui a été extrait à l'étape 1.

Format générique de sortie, quel que soit le regroupement fourni :
  <data_dir>/disciplines/<classement>.parquet
      topic_id, discipline_code, discipline_nom, domaine_code, doute,
      discipline_code_alternative
  -> domaine_code = lettres en tête du code (ERC : LS, PE, SH)
  -> un topic = une discipline. Les étapes suivantes font simplement
     works.topic_id = disciplines.topic_id.

Fichiers acceptés en entrée :
  - le JSON ERC ({"topics": [...]} ou une liste) ;
  - tout CSV / JSON avec une colonne topic et une colonne code de discipline
    (noms de colonnes passés en argument).

Exemples :
    from project.server.main.disciplines import build_disciplines
    res = build_disciplines()                                   # ERC, valeurs de config.py
    res = build_disciplines(classement="erc_alt", use_alternative=True)  # test de sensibilité
    res = build_disciplines("topics/mon_regroupement.csv", classement="ost",
                            code_col="code", name_col="libelle")
"""

import json
from pathlib import Path

import duckdb
import pandas as pd

from .config import DATA_DIR, CLASSEMENT, TOPICS_PATH

_VRAI = {"true", "1", "oui", "yes", "vrai"}


def load_mapping(path: str = TOPICS_PATH,
                 topic_col: str = "topic_id",
                 code_col: str = "erc_panel",
                 name_col: str = "erc_panel_name",
                 alt_col: str = "panel_alternatif",
                 doubt_col: str = "doute",
                 use_alternative: bool = False) -> pd.DataFrame:
    """Lit un fichier de correspondance et renvoie le format générique.

    use_alternative : True -> pour les topics marqués « doute », on prend le
                      panel alternatif quand il existe (test de sensibilité).
    Les colonnes alt_col / doubt_col sont facultatives (ignorées si absentes).
    """
    path = Path(path)
    if path.suffix.lower() == ".json":
        raw = json.loads(path.read_text(encoding="utf-8"))
        df = pd.DataFrame(raw["topics"] if isinstance(raw, dict) else raw)
    else:
        df = pd.read_csv(path, sep=None, engine="python", dtype=str, encoding="utf-8-sig")

    manquantes = [c for c in (topic_col, code_col) if c not in df.columns]
    if manquantes:
        raise ValueError(f"{path} : colonnes absentes {manquantes} ; "
                         f"colonnes disponibles : {list(df.columns)}")

    def clean(col):
        s = df[col].astype("string").str.strip()
        return s.mask(s == "")

    out = pd.DataFrame({
        # 'T10001' ou 'https://openalex.org/T10001' -> 'T10001'
        "topic_id": clean(topic_col).str.extract(r"([^/]+)$")[0].str.upper(),
        "discipline_code": clean(code_col),
        "discipline_nom": clean(name_col) if name_col in df.columns else clean(code_col),
        "doute": (df[doubt_col].astype(str).str.strip().str.lower().isin(_VRAI)
                  if doubt_col in df.columns else False),
        "discipline_code_alternative": clean(alt_col) if alt_col in df.columns else pd.NA,
    })

    if use_alternative:
        noms = (out.dropna(subset=["discipline_code"])
                   .drop_duplicates("discipline_code")
                   .set_index("discipline_code")["discipline_nom"])
        m = out["doute"] & out["discipline_code_alternative"].notna()
        alt = out.loc[m, "discipline_code_alternative"]
        out.loc[m, "discipline_nom"] = alt.map(noms).fillna(alt)
        out.loc[m, "discipline_code"] = alt

    out = out.dropna(subset=["topic_id"])
    out["domaine_code"] = out["discipline_code"].str.extract(r"^([A-Za-z]+)")[0].str.upper()
    out = out[["topic_id", "discipline_code", "discipline_nom", "domaine_code", "doute",
               "discipline_code_alternative"]]

    # Un topic ne doit appartenir qu'à une seule discipline
    conflits = (out.dropna(subset=["discipline_code"])
                   .groupby("topic_id")["discipline_code"].nunique())
    conflits = conflits[conflits > 1]
    if len(conflits):
        raise ValueError(f"{len(conflits)} topic(s) rattaché(s) à plusieurs disciplines : "
                         f"{list(conflits.index[:10])}")
    return out.drop_duplicates("topic_id").reset_index(drop=True)


def check_coverage(classement: str = CLASSEMENT, data_dir: str = DATA_DIR) -> dict:
    """Confronte le classement aux articles extraits (étape 1).

    Renvoie :
      nb_articles, nb_articles_classes, pct_classes   couverture des articles
      topics_non_classes   DataFrame : topics présents dans les articles mais
                           sans discipline (ou sans code), triés par volume
      n_topics_absents     topics du fichier jamais vus dans les articles
      par_discipline       DataFrame : volume et % d'articles « doute » par discipline
      par_discipline_annee DataFrame long : discipline x année
    """
    data_dir = Path(data_dir)
    con = duckdb.connect()
    con.execute(f"""
        CREATE TEMP VIEW w AS
        SELECT upper(topic_id) AS topic_id, year AS annee_publication
        FROM read_parquet('{data_dir}/works/*.parquet')
    """)
    con.execute(f"""
        CREATE TEMP TABLE m AS
        SELECT * FROM read_parquet('{data_dir}/disciplines/{classement}.parquet')
        WHERE discipline_code IS NOT NULL
    """)

    # Couverture : LEFT JOIN des articles sur le classement ; count(m.topic_id) ne
    # compte que les articles dont le topic a trouvé une discipline (NULL sinon).
    # ANTI JOIN (ci-dessous) : les lignes de gauche SANS correspondance à droite,
    # c'est-à-dire les topics des articles absents du classement, et inversement.
    n, n_ok = con.execute("""
        SELECT count(*), count(m.topic_id)
        FROM w LEFT JOIN m USING (topic_id)
    """).fetchone()

    res = {
        "nb_articles": n,
        "nb_articles_classes": n_ok,
        "pct_classes": round(100 * n_ok / n, 3) if n else None,
        "topics_non_classes": con.execute("""
            SELECT topic_id, count(*) AS nb_articles
            FROM w ANTI JOIN m USING (topic_id)
            GROUP BY 1 ORDER BY nb_articles DESC
        """).df(),
        "n_topics_absents": con.execute("""
            SELECT count(*) FROM m ANTI JOIN (SELECT DISTINCT topic_id FROM w) USING (topic_id)
        """).fetchone()[0],
        "par_discipline": con.execute("""
            SELECT discipline_code, discipline_nom,
                   count(*)                                     AS nb_articles,
                   round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct_articles,
                   round(100 * avg(doute::INT), 1)              AS pct_doute
            FROM w JOIN m USING (topic_id)
            GROUP BY 1, 2 ORDER BY 1
        """).df(),
        "par_discipline_annee": con.execute("""
            SELECT discipline_code, annee_publication, count(*) AS nb_articles
            FROM w JOIN m USING (topic_id)
            GROUP BY 1, 2 ORDER BY 1, 2
        """).df(),
    }
    con.close()
    return res


def build_disciplines(mapping_path: str = TOPICS_PATH,
                      classement: str = CLASSEMENT,
                      data_dir: str = DATA_DIR,
                      verbose: bool = True,
                      **load_kwargs) -> dict:
    """Charge le classement, l'écrit dans <data_dir>/disciplines/<classement>.parquet
    et renvoie les contrôles de couverture (voir check_coverage).
    load_kwargs : arguments de load_mapping (code_col, name_col, use_alternative...)."""
    mapping = load_mapping(mapping_path, **load_kwargs)
    out_dir = Path(data_dir) / "disciplines"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{classement}.parquet"
    # écriture via DuckDB (évite d'ajouter pyarrow aux dépendances)
    for c in ("discipline_code_alternative", "domaine_code"):
        mapping[c] = mapping[c].astype("string")
    with duckdb.connect() as con:
        con.register("mapping", mapping)
        con.execute(f"COPY mapping TO '{out_file}' (FORMAT parquet)")

    res = {"fichier": str(out_file),
           "n_topics": len(mapping),
           "n_sans_code": int(mapping["discipline_code"].isna().sum()),
           "n_disciplines": mapping["discipline_code"].nunique(),
           "n_doute": int(mapping["doute"].sum())}
    res.update(check_coverage(classement, data_dir))

    if verbose:
        print(f"{classement} : {res['n_topics']} topics -> {res['n_disciplines']} disciplines "
              f"({res['n_doute']} topics en doute, {res['n_sans_code']} sans code) -> {out_file}")
        print(f"Articles classés : {res['nb_articles_classes']:,} / {res['nb_articles']:,} "
              f"({res['pct_classes']} %)")
        nc = res["topics_non_classes"]
        if len(nc):
            print(f"{len(nc)} topic(s) présents dans les articles sans discipline "
                  f"({nc['nb_articles'].sum():,} articles) ; les plus fréquents :")
            print(nc.head(10).to_string(index=False))
        if res["n_topics_absents"]:
            print(f"{res['n_topics_absents']} topic(s) du fichier absents des articles extraits")
        print()
        print(res["par_discipline"].to_string(index=False))
    return res
