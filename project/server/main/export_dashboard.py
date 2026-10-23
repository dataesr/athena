"""
export_dashboard.py
===================
Aperçu HTML : une page autonome (DSFR) pour vérifier les résultats sans
l'application tableaux. Même contrat de données que publish.py :
  - effectifs : <data_dir>/indicateurs/<classement>_effectifs.parquet (étape 6)
  - entités   : entites.construire_entites() (noms, alias, sélecteur)
  - méthode   : publish.meta_bibliometrie()

Les effectifs sont compactés en colonnes (perimetre, entite et discipline remplacés
par leur position dans les listes meta.perimetres, entites et disciplines), puis
compressés (gzip) et inclus dans la page en base64. Les ratios sont recalculés dans
le navigateur : on additionne les effectifs, puis on divise.

Sorties (dans out_dir, défaut <data_dir>/apercu/) :
  index.html              la page complète
  bibliometrie.json.gz    les mêmes données, à part

Exemple :
    from project.server.main.export_dashboard import build_dashboard
    res = build_dashboard()
"""

import base64
import gzip
import json
from pathlib import Path

import duckdb
import pandas as pd

from .common import MONDE_ID, PAYS_UE, REGIONS
from .config import CLASSEMENT, DASHBOARD_ETABS_PATH, DATA_DIR, REFERENTIEL_DIR, REFERENTIEL_SOURCE
from .entites import construire_entites
from .publish import _cle_code, meta_bibliometrie

TEMPLATE = Path(__file__).parent / "dashboard" / "index_template.html"
DSFR_URL = "https://cdn.jsdelivr.net/npm/@gouvfr/dsfr@1.15.3"
CHARTJS_URL = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"
CHAMPS = ["nb_articles", "nb_articles_top10_cites", "nb_articles_revues_tete_disciplinaires",
          "nb_articles_revues_tete_multidisciplinaires"]


def build_dashboard(classement: str = CLASSEMENT,
                    data_dir: str = DATA_DIR,
                    out_dir: str = None,
                    titre: str = "Indicateurs de la recherche",
                    sous_titre: str = "Bibliométrie, financements, innovation",
                    dsfr_url: str = DSFR_URL,
                    chartjs_url: str = CHARTJS_URL,
                    template: str = None,
                    etabs_path: str = DASHBOARD_ETABS_PATH,
                    referentiel_source: str = REFERENTIEL_SOURCE,
                    referentiel_dir: str = REFERENTIEL_DIR,
                    verbose: bool = True) -> dict:
    """Produit <out_dir>/index.html (page autonome) et bibliometrie.json.gz.
    etabs_path : établissements du sélecteur (None = tous les français)."""
    data_dir = Path(data_dir)
    out_dir = Path(out_dir) if out_dir else data_dir / "apercu"
    out_dir.mkdir(parents=True, exist_ok=True)
    f_eff = data_dir / "indicateurs" / f"{classement}_effectifs.parquet"
    if not f_eff.exists():
        raise FileNotFoundError(f"{f_eff} absent : lancer compute_indicators()")

    meta = meta_bibliometrie(classement, data_dir)
    perimetres = meta["perimetres"]

    disc = duckdb.sql(f"""SELECT DISTINCT discipline_code, discipline_nom
                          FROM read_parquet('{data_dir}/disciplines/{classement}.parquet')
                          WHERE discipline_code IS NOT NULL""").fetchall()
    disc = sorted(disc, key=lambda r: _cle_code(r[0]))
    i_disc = {c: k for k, (c, _) in enumerate(disc)}

    # Entités (hors monde) : France, autres pays retenus, établissements français, étrangers
    ent = construire_entites(classement, data_dir, etabs_path, referentiel_source, referentiel_dir,
                             verbose=verbose)
    ent = ent[(ent["entite_type"] == "etablissement") | ent["pays_compare"]].copy()
    ent["rang"] = [0 if i == "FR" else 1 if t == "pays" else 2 if p == "FR" else 3
                   for i, t, p in zip(ent["entite_id"], ent["entite_type"], ent["pays_code"])]
    ent = ent.sort_values(["rang", "nb_articles"], ascending=[True, False]).reset_index(drop=True)
    i_ent = {e: k for k, e in enumerate(ent["entite_id"])}
    entites = [{"entite_id": r.entite_id, "nom": r.nom, "entite_type": r.entite_type,
                "nb_articles": int(r.nb_articles), "pays_code": r.pays_code or "",
                "alias": list(r.alias), "selecteur": bool(r.selecteur),
                "ordre_selecteur": None if pd.isna(r.ordre_selecteur) else int(r.ordre_selecteur)}
               for r in ent.itertuples()]

    eff = duckdb.sql(f"SELECT * FROM read_parquet('{f_eff}') WHERE classement = '{classement}'").df()
    eff["perimetre"] = eff["perimetre"].map({p: k for k, p in enumerate(perimetres)})
    eff["discipline"] = eff["discipline_code"].map(i_disc)
    eff["nb_articles_top10_cites"] = eff["nb_articles_top10_cites"].round(2)

    def colonnes(df, cles):
        return {c: df[c].tolist() for c in cles}

    monde = eff[eff["entite_id"] == MONDE_ID]
    autres = eff[eff["entite_id"].isin(i_ent)].copy()
    autres["entite"] = autres["entite_id"].map(i_ent)
    autres = autres.sort_values(["entite", "perimetre", "annee_publication", "discipline"])
    payload = {
        "meta": {**{k: meta[k] for k in ("perimetres", "annees", "annees_influence_provisoire",
                                         "nb_revues_tete", "revues_multidisciplinaires",
                                         "methode", "sources", "snapshot_openalex", "date_calcul")},
                 "classement": classement, "pays_ue": PAYS_UE,
                 "regions": {c: r for r, codes in REGIONS.items() for c in codes}},
        "disciplines": [[c, n] for c, n in disc],
        "entites": entites,
        "effectifs_monde": colonnes(monde, ["perimetre", "annee_publication", "discipline"] + CHAMPS),
        "effectifs": colonnes(autres, ["perimetre", "entite", "annee_publication", "discipline"] + CHAMPS),
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
           "nb_entites": len(entites), "nb_lignes": len(autres),
           "nb_selecteur": int(ent["selecteur"].sum())}
    if verbose:
        print(f"{f_html} : {res['taille_mo']} Mo ({res['nb_entites']:,} entités, "
              f"{res['nb_lignes']:,} lignes ; JSON {res['json_mo']} Mo -> gzip {res['gzip_mo']} Mo)")
    return res
