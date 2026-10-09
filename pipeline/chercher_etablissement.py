#!/usr/bin/env python3
"""Recherche un établissement dans OpenAlex et liste les fiches candidates.

Usage :
  python pipeline/chercher_etablissement.py "Sciences Po"
  python pipeline/chercher_etablissement.py "Université de Guyane" "Toulouse Capitole"
  python pipeline/chercher_etablissement.py I80043              # vérifier un identifiant

Pour chaque candidat : identifiant, nom, ville, type, nombre de publications et
institution « parente » éventuelle. Reporter ensuite le bon identifiant dans la
colonne openalex_id de config/etablissements.csv (il devient prioritaire).
Utilise OPENALEX_API_KEY ; quelques requêtes seulement, sans cache.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from openalex import OpenAlex  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
FIELDS = "id,display_name,works_count,type,geo,country_code,lineage,associated_institutions"


def show(r: dict, names: dict[str, str]) -> None:
    oid = r["id"].split("/")[-1]
    geo = r.get("geo") or {}
    parents = [x for x in (r.get("lineage") or []) if x.split("/")[-1] != oid]
    parent_txt = ""
    if parents:
        parent_txt = " · rattachée à : " + ", ".join(names.get(p.split("/")[-1], p.split("/")[-1]) for p in parents)
    works = f"{r.get('works_count') or 0:,}".replace(",", " ")
    print(f"  {oid:<13} {works:>9} publ.  {r.get('display_name', '')}"
          f"  [{r.get('type', '?')}, {geo.get('city') or '?'}, {r.get('country_code') or '?'}]{parent_txt}")


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    params = json.loads((ROOT / "config" / "params.json").read_text())
    countries = "|".join(c.lower() for c in params.get("codes_pays_france", ["FR"]))
    oa = OpenAlex(cache_dir=ROOT / "data" / "cache" / "recherche_etablissements", rps=2)
    for q in sys.argv[1:]:
        print(f"\n« {q} »")
        if q.upper().startswith("I") and q[1:].isdigit():
            res = [oa.get(f"/institutions/{q.upper()}", {"select": FIELDS}, use_cache=False)]
        else:
            res = oa.get("/institutions", {"search": q, "filter": f"country_code:{countries}", "per_page": 8,
                                           "select": FIELDS}, use_cache=False).get("results", [])
        if not res:
            print("  aucun résultat")
            continue
        parent_ids = {p.split("/")[-1] for r in res for p in (r.get("lineage") or [])} - {r["id"].split("/")[-1] for r in res}
        names = {r["id"].split("/")[-1]: r["display_name"] for r in res}
        if parent_ids:
            pr = oa.get("/institutions", {"filter": "openalex:" + "|".join(sorted(parent_ids)[:50]),
                                          "select": "id,display_name", "per_page": 50}, use_cache=False)
            names.update({r["id"].split("/")[-1]: r["display_name"] for r in pr.get("results", [])})
        for r in sorted(res, key=lambda r: -(r.get("works_count") or 0)):
            show(r, names)
    print("\nReportez l'identifiant retenu dans la colonne openalex_id de config/etablissements.csv.")


if __name__ == "__main__":
    main()
