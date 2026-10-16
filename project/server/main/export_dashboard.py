"""
export_dashboard.py
===================
Étape 7 : le dashboard, sous la forme d'UNE page HTML autonome (DSFR).

Les indicateurs de l'étape 6 sont compactés puis inclus dans la page :
  - les totaux mondiaux sont stockés une seule fois par monde x année x discipline ;
  - pour chaque entité (France, établissements), seuls les COMPTES sont gardés
    (articles, poids top 10 %, articles en revues de tête) ; les lignes
    « toutes disciplines » sont retirées (ce sont des sommes) ;
  - les ratios (poids, influence, prestige) sont recalculés dans le navigateur, pour
    n'importe quelle période ou discipline : on somme les comptes, puis on divise ;
  - le tout est compressé (gzip) et encodé en base64 dans la page.

Aucune base ni API : la page s'ouvre telle quelle (fichier local, serveur web,
envoi par mail). Elle charge seulement le DSFR et Chart.js depuis un CDN
(dsfr_url / chartjs_url, à remplacer par une copie locale si besoin).

Sorties (dans out_dir, défaut <data_dir>/dashboard/) :
  index.html              la page complète
  bibliometrie.json.gz    les mêmes données, à part (pour une future version
                          où la page irait chercher ses données au lieu de les inclure)

Sélecteur d'établissements : fichier CSV config.DASHBOARD_ETABS_PATH
(voir load_etab_selection). Fichier de départ, à élaguer :
    write_etab_selection()

Recherche dans le dashboard : nom, ROR et alias. Alias = sigles et noms
alternatifs OpenAlex (si l'étape 2 les a extraits), sigle calculé à partir du
nom (« Centre National de la Recherche Scientifique » -> CNRS) et colonne
alias du fichier de configuration.

Exemple :
    from project.server.main.export_dashboard import build_dashboard
    res = build_dashboard()
    res["fichier"], res["taille_mo"]
"""

import base64
import datetime as dt
import gzip
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .common import PAYS_NOMS, PAYS_UE, REGIONS
from .config import DASHBOARD_ETABS_PATH, DATA_DIR, DISCIPLINES_NAME, SJR_PATH

TEMPLATE = Path(__file__).parent / "dashboard" / "index_template.html"
DSFR_URL = "https://cdn.jsdelivr.net/npm/@gouvfr/dsfr@1.15.3"
CHARTJS_URL = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"


def _code_key(code: str):
    """Tri naturel des codes : LS1, LS2, ..., LS9, PE1, ..., PE10, PE11, SH1..."""
    m = re.match(r"([A-Za-z]*)(\d*)(.*)", str(code))
    return (m.group(1), int(m.group(2)) if m.group(2) else 0, m.group(3))


_MOTS_VIDES = {"de", "du", "des", "la", "le", "les", "l", "d", "et", "en", "pour", "sur", "aux", "au",
               "a", "à", "of", "the", "and", "for", "in", "at", "y", "del", "der", "und", "für"}


def _sigle(nom: str):
    """Sigle calculé à partir des initiales des mots significatifs :
    'Centre National de la Recherche Scientifique' -> 'CNRS'. None si < 2 lettres."""
    mots = [m for m in re.split(r"[\s\-'’,()/]+", str(nom or "")) if m]
    init = "".join(m[0] for m in mots if m.lower() not in _MOTS_VIDES and m[0].isalpha())
    return init.upper() if len(init) >= 2 else None


def load_etab_selection(path: str = DASHBOARD_ETABS_PATH) -> pd.DataFrame:
    """Lit le fichier des établissements du sélecteur.

    Format : CSV, séparateur « ; » ou « , », lignes commençant par # ignorées.
      ror    obligatoire : ROR court (02feahw73) ou URL (https://ror.org/02feahw73)
      nom    facultatif : nom affiché (sinon le nom OpenAlex)
      alias  facultatif : termes de recherche supplémentaires, séparés par |
             (ex. « CNRS|Centre national de la recherche »)
    Les autres colonnes (n_articles, type...) sont ignorées. L'ordre des lignes
    est l'ordre d'affichage quand la recherche est vide.
    """
    p = Path(path)
    lignes = [l for l in p.read_text(encoding="utf-8-sig").splitlines() if l.strip() and not l.lstrip().startswith("#")]
    if not lignes:
        return pd.DataFrame(columns=["ror", "nom", "alias"])
    sep = ";" if lignes[0].count(";") >= lignes[0].count(",") else ","
    import io
    df = pd.read_csv(io.StringIO("\n".join(lignes)), sep=sep, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    if "ror" not in df.columns:
        raise ValueError(f"{p} : colonne « ror » absente (colonnes lues : {list(df.columns)})")
    for c in ("nom", "alias"):
        if c not in df.columns:
            df[c] = ""
    df["ror"] = df["ror"].str.strip().str.rstrip("/").str.split("/").str[-1]
    df = df[df["ror"] != ""].drop_duplicates("ror")
    return df[["ror", "nom", "alias"]].map(lambda v: str(v).strip())


def write_etab_selection(path: str = DASHBOARD_ETABS_PATH, disciplines: str = DISCIPLINES_NAME,
                         data_dir: str = DATA_DIR, min_articles: int = 0,
                         overwrite: bool = False) -> str:
    """Écrit un fichier de départ avec TOUS les établissements français des
    indicateurs (du plus gros au plus petit), à élaguer à la main.
    Colonnes : ror ; nom (à remplir pour changer le nom affiché) ; alias ;
    n_articles et type (pour information, ignorées à la lecture)."""
    p = Path(path)
    if p.exists() and not overwrite:
        raise FileExistsError(f"{p} existe déjà (overwrite=True pour l'écraser)")
    f_etab = Path(data_dir) / "indicators" / f"{disciplines}_etablissements.parquet"
    df = duckdb.sql(f"""
        SELECT ror, '' AS nom, '' AS alias, n_articles, type, nom AS nom_openalex
        FROM read_parquet('{f_etab}')
        WHERE pays = 'FR' AND n_articles >= {int(min_articles)}
        ORDER BY n_articles DESC
    """).df()
    p.parent.mkdir(parents=True, exist_ok=True)
    entete = ("# Établissements proposés dans le sélecteur du dashboard (une ligne = un établissement).\n"
              "# Supprimer les lignes inutiles ; l'ordre des lignes = ordre d'affichage.\n"
              "# nom : nom affiché (vide = nom OpenAlex) ; alias : termes de recherche séparés par |.\n"
              "# n_articles, type, nom_openalex : pour information, ignorés à la lecture.\n")
    p.write_text(entete + df.to_csv(sep=";", index=False), encoding="utf-8")
    print(f"{p} : {len(df):,} établissements")
    return str(p)


def _read_json(p: Path) -> dict:
    return json.loads(p.read_text()) if p.exists() else {}


def _methode(p_ind: dict, p_top: dict, p_tj: dict) -> list:
    """Textes de la note méthodologique, tirés des paramètres réellement utilisés."""
    w = p_top.get("citation_window")
    first = (p_top.get("counts_years") or [None])[0]
    types = ", ".join(p_top.get("work_types") or []) or "tous types"
    lignes = [
        f"Périmètre : documents OpenAlex de type « {types} » parus dans une revue, "
        f"publiés de {p_ind.get('annee_min')} à {p_ind.get('annee_max')}, hors rétractations ; "
        f"chaque article est classé dans une seule discipline (topic principal OpenAlex -> panel ERC).",
        "Comptage entier : un article compte pour la France s'il a au moins un auteur affilié "
        "en France (métropole ou outre-mer), et pour un établissement si l'une de ses "
        "affiliations est cet établissement ou l'une de ses structures rattachées (lignée OpenAlex, ROR).",
        "Monde de référence : soit toutes les revues OpenAlex, soit les seules revues indexées "
        "dans SJR (proche du périmètre Scopus). Le choix modifie le poids et l'influence, pas le prestige.",
        (f"Influence : part de l'établissement dans les {round(100 * (p_top.get('top_share') or .1))} % "
         f"d'articles les plus cités de chaque discipline et année. Citations reçues l'année de "
         f"publication et les {w} années suivantes"
         + (f" (années antérieures à {first} : citations cumulées)" if first else "")
         + "; ex aequo au seuil répartis au prorata ; articles jamais cités exclus du top."
         if w is not None else
         "Influence : part dans le top 10 % des articles les plus cités (citations cumulées)."),
        "Top 10 % selon la méthode de Waltman et Schreiber (2013) : il contient exactement 10 % des "
        "articles de chaque discipline et année, compté en équivalent-articles (d'où des décimales) ; "
        "les articles à égalité au seuil de citations se partagent les places restantes au prorata "
        "(3 ex aequo pour 1 place : 1/3 chacun). Réf. : L. Waltman, M. Schreiber, « On the calculation "
        "of percentile-based bibliometric indicators », JASIST 64(2), 2013, p. 372-379.",
        "Années provisoires : la fenêtre de citation n'est pas encore close, l'influence peut évoluer.",
        (f"Prestige : part de l'établissement dans les articles d'une discipline parus dans ses "
         f"{p_tj.get('n_top', 10)} revues de tête (SJR le plus élevé parmi les revues dont au moins "
         f"{round(100 * (p_tj.get('min_share') or 0))} % des articles relèvent de la discipline, "
         f"hors revues de synthèse)"
         + (f", plus les revues multidisciplinaires {', '.join(p_tj.get('multidisc_retenues') or [])}"
            if p_tj.get("multidisc_retenues") else "") + "."),
        "Matrice de positionnement : niveau = part sur la période d'arrivée ; dynamique = évolution "
        "relative (part d'arrivée ÷ part de départ − 1) ou écart en points ; repère vertical = part de "
        "l'entité toutes disciplines (points = disciplines) ou médiane (points = pays ou établissements). "
        "Peu fiable sous 30 articles.",
        "Agrégation : pour une période ou un ensemble de disciplines, les effectifs sont additionnés "
        "avant le calcul des parts (pas de moyenne de pourcentages).",
    ]
    return lignes


def build_dashboard(disciplines: str = DISCIPLINES_NAME,
                    data_dir: str = DATA_DIR,
                    out_dir: str = None,
                    titre: str = "Indicateurs de la recherche",
                    sous_titre: str = "Bibliométrie, financements, innovation",
                    dsfr_url: str = DSFR_URL,
                    chartjs_url: str = CHARTJS_URL,
                    template: str = None,
                    etabs_path: str = DASHBOARD_ETABS_PATH,
                    verbose: bool = True) -> dict:
    """Produit <out_dir>/index.html (page autonome) et bibliometrie.json.gz.
    etabs_path : établissements du sélecteur (None = tous les français)."""
    data_dir = Path(data_dir)
    out_dir = Path(out_dir) if out_dir else data_dir / "dashboard"
    out_dir.mkdir(parents=True, exist_ok=True)
    f_ind = data_dir / "indicators" / f"{disciplines}.parquet"
    f_etab = data_dir / "indicators" / f"{disciplines}_etablissements.parquet"
    if not f_ind.exists():
        raise FileNotFoundError(f"{f_ind} absent : lancer compute_indicators()")

    con = duckdb.connect()
    con.execute(f"CREATE TEMP TABLE ind AS SELECT * FROM read_parquet('{f_ind}') "
                f"WHERE discipline_code <> 'ALL'")

    # Disciplines (tri naturel) et entités (France puis établissements par taille)
    disc = con.execute("SELECT DISTINCT discipline_code, discipline_name FROM ind").fetchall()
    disc = sorted(disc, key=lambda r: _code_key(r[0]))
    con.execute("CREATE TEMP TABLE disc_idx AS SELECT * FROM (VALUES "
                + ", ".join(f"('{c}', {i})" for i, (c, _) in enumerate(disc))
                + ") t(discipline_code, d)")

    # Entités : [id, nom, type, articles (monde "all", toute la période), pays]
    #   France, puis les autres pays, puis les établissements français, puis étrangers
    etab_meta = {}
    if f_etab.exists():
        etab_meta = {r[0]: r[1:] for r in con.execute(
            f"SELECT ror, coalesce(nom, ror), coalesce(pays, ''), coalesce(type, '') "
            f"FROM read_parquet('{f_etab}')").fetchall()}
    rows = con.execute("""
        SELECT entity_type, entity_id, any_value(entity_name),
               sum(n_ent) FILTER (WHERE world = 'all')
        FROM ind GROUP BY 1, 2
    """).fetchall()
    entites = []
    for et, eid, nom, n in rows:
        if et == "pays":
            entites.append([eid, PAYS_NOMS.get(eid, nom or eid), "pays", int(n or 0), eid])
        else:
            nom_e, pays_e, type_e = etab_meta.get(eid, (nom or eid, "", ""))
            entites.append([eid, nom_e, type_e or "etablissement", int(n or 0), pays_e])
    # Sélecteur : fichier de configuration (noms affichés, alias)
    sel = None
    if etabs_path and Path(etabs_path).exists():
        sel = load_etab_selection(etabs_path)
    elif etabs_path and verbose:
        print(f"{etabs_path} absent : tous les établissements français sont proposés "
              f"(write_etab_selection() crée un fichier de départ)")
    noms_cfg = {r.ror: r.nom for r in sel.itertuples() if r.nom} if sel is not None else {}
    for e in entites:
        if e[0] in noms_cfg:
            e[1] = noms_cfg[e[0]]

    # Alias de recherche : sigles et noms alternatifs OpenAlex, sigle calculé, alias du fichier
    alias = {}
    f_inst = data_dir / "institutions.parquet"
    oa = {}
    if f_inst.exists():
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f_inst}')").fetchall()}
        if "display_name_acronyms" in cols:
            alt = "display_name_alternatives" if "display_name_alternatives" in cols else "[]::VARCHAR[]"
            oa = {r[0]: (list(r[1] or []), list(r[2] or [])) for r in con.execute(f"""
                SELECT ror, display_name_acronyms, {alt} FROM read_parquet('{f_inst}')
                WHERE ror IS NOT NULL""").fetchall()}
    cfg_alias = {r.ror: [a.strip() for a in r.alias.split("|") if a.strip()]
                 for r in sel.itertuples()} if sel is not None else {}
    for e in entites:
        if e[2] == "pays":
            continue
        acr, alts = oa.get(e[0], ([], []))
        cands = cfg_alias.get(e[0], []) + acr + [_sigle(e[1])] + [a for a in alts if len(a) <= 80][:6]
        vus, garde = {e[1].lower()}, []
        for a in cands:
            if a and a.lower() not in vus:
                vus.add(a.lower()); garde.append(a)
        if garde:
            alias[e[0]] = "|".join(garde)

    rang = lambda e: (0 if e[0] == "FR" else 1 if e[2] == "pays" else
                      2 if e[4] == "FR" else 3, -e[3])
    entites.sort(key=rang)
    con.register("ent_list", pd.DataFrame(
        {"entity_id": [e[0] for e in entites], "e": range(len(entites))}))

    def colonnes(sql: str, noms: list) -> dict:
        rows = con.execute(sql).fetchall()
        cols = list(zip(*rows)) if rows else [[] for _ in noms]
        return {k: list(v) for k, v in zip(noms, cols)}

    monde = colonnes("""
        SELECT DISTINCT (world = 'sjr')::INT, year, di.d, n_monde, round(top_monde, 2),
               tj_monde, tj_monde_disc
        FROM ind JOIN disc_idx di USING (discipline_code)
        ORDER BY 1, 2, 3
    """, ["w", "y", "d", "n", "top", "tj", "tjd"])
    ent = colonnes("""
        SELECT (i.world = 'sjr')::INT, el.e, i.year, di.d, i.n_ent, round(i.top_ent, 2),
               i.tj_ent, i.tj_ent_disc
        FROM ind i
        JOIN disc_idx di USING (discipline_code)
        JOIN ent_list el ON el.entity_id = i.entity_id
        ORDER BY 2, 1, 3, 4
    """, ["w", "e", "y", "d", "n", "top", "tj", "tjd"])

    selecteur = None
    if sel is not None:
        presents = {e[0] for e in entites if e[2] != "pays" and e[4] == "FR"}
        selecteur = [r for r in sel["ror"] if r in presents]
        absents = [r for r in sel["ror"] if r not in presents]
        if verbose:
            print(f"Sélecteur : {len(selecteur):,} établissements ({etabs_path})")
            if absents:
                print(f"  {len(absents)} ROR absents des indicateurs (pas français, sous ETAB_MIN_ARTICLES, "
                      f"exclus ou mal saisis) : {', '.join(absents[:20])}{' ...' if len(absents) > 20 else ''}")

    annees = [r[0] for r in con.execute("SELECT DISTINCT year FROM ind ORDER BY 1").fetchall()]
    prov = [r[0] for r in con.execute(
        "SELECT DISTINCT year FROM ind WHERE provisoire ORDER BY 1").fetchall()]
    con.close()

    # Paramètres des étapes précédentes (note méthodologique, sources)
    p_ind = _read_json(data_dir / "indicators" / f"{disciplines}_params.json")
    p_top = _read_json(data_dir / "top10" / f"{disciplines}_params.json")
    p_tj = _read_json(data_dir / "top_journals" / f"{disciplines}_params.json")
    man = _read_json(data_dir / "works_manifest.json")
    snapshot = f"{man.get('source', 'public')} du {man.get('date', '?')}"
    genere = dt.date.today().isoformat()

    payload = {
        "meta": {
            "annees": annees,
            "annees_provisoires": prov,
            "multidisciplinaires": p_tj.get("multidisc_retenues") or [],
            "n_revues_tete": p_tj.get("n_top", 10),
            "methode": _methode(p_ind, p_top, p_tj),
            "sources": (f"Sources : OpenAlex (snapshot {snapshot}), SCImago Journal Rank "
                        f"({Path(p_tj.get('sjr_path') or SJR_PATH).name}). "
                        f"Indicateurs calculés le {genere}."),
            "snapshot": snapshot, "genere_le": genere, "regroupement": disciplines,
            "pays_ue": PAYS_UE,
            "selecteur": selecteur,
            "alias": alias,
            "regions": {c: r for r, codes in REGIONS.items() for c in codes},
        },
        "disciplines": [[c, n] for c, n in disc],
        "entites": entites,
        "monde": monde,
        "ent": ent,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    gz = gzip.compress(raw, compresslevel=9)
    (out_dir / "bibliometrie.json.gz").write_bytes(gz)

    tpl = Path(template or TEMPLATE).read_text(encoding="utf-8")
    html = (tpl.replace("{{DATA_B64}}", base64.b64encode(gz).decode("ascii"))
               .replace("{{DSFR_URL}}", dsfr_url.rstrip("/"))
               .replace("{{CHARTJS_URL}}", chartjs_url)
               .replace("{{TITRE}}", titre)
               .replace("{{SOUS_TITRE}}", sous_titre))
    f_html = out_dir / "index.html"
    f_html.write_text(html, encoding="utf-8")

    res = {"fichier": str(f_html), "taille_mo": round(f_html.stat().st_size / 1e6, 2),
           "json_mo": round(len(raw) / 1e6, 1), "gzip_mo": round(len(gz) / 1e6, 2),
           "entites": len(entites), "lignes": len(ent["e"]), "snapshot": snapshot,
           "selecteur": len(selecteur) if selecteur is not None else None}
    if verbose:
        print(f"{f_html} : {res['taille_mo']} Mo ({res['entites']:,} entités, "
              f"{res['lignes']:,} lignes ; JSON {res['json_mo']} Mo -> gzip {res['gzip_mo']} Mo)")
    return res
