#!/usr/bin/env python3
"""Génère un jeu de données SYNTHÉTIQUE au format de extract_openalex.py, pour tester
le tableau de bord sans OpenAlex. Les chiffres sont fictifs ; leurs ordres de grandeur
imitent ceux d'OpenAlex en compte entier (part de la France dans le top 10 % ≈ 20 %)."""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

WORLD_W = {"A": 0.06, "B": 0.17, "C": 0.30, "D": 0.12, "E": 0.11, "F": 0.035, "G": 0.035}
# pays : part de la production mondiale, croissance annuelle, niveau d'impact (compte entier),
# tendance annuelle de l'impact
COUNTRIES = {
    "US": (0.150, 0.010, 1.70, -0.012), "CN": (0.260, 0.070, 1.05, 0.040), "IN": (0.060, 0.060, 0.70, 0.015),
    "GB": (0.048, 0.012, 1.85, -0.004), "DE": (0.047, 0.008, 1.62, -0.002), "JP": (0.032, -0.005, 0.95, -0.008),
    "IT": (0.036, 0.015, 1.52, 0.010), "CA": (0.029, 0.010, 1.70, -0.006), "AU": (0.027, 0.020, 1.75, 0.000),
    "ES": (0.031, 0.020, 1.48, 0.006), "KR": (0.030, 0.020, 1.12, 0.012), "BR": (0.025, 0.020, 0.75, -0.005),
    "NL": (0.018, 0.010, 2.00, -0.006), "CH": (0.015, 0.012, 2.05, -0.005), "SE": (0.012, 0.010, 1.85, -0.004),
    "BE": (0.010, 0.010, 1.80, -0.003), "DK": (0.009, 0.015, 1.90, 0.000), "AT": (0.008, 0.012, 1.70, 0.000),
    "RU": (0.020, 0.010, 0.55, 0.010), "TR": (0.021, 0.035, 0.70, 0.010), "IR": (0.020, 0.040, 0.85, 0.015),
    "TW": (0.012, 0.000, 0.95, 0.005), "PL": (0.018, 0.020, 0.95, 0.010), "SA": (0.010, 0.080, 1.10, 0.040),
}
FR_PARAMS = (0.027, 0.004, 1.72, -0.012)
FR_MACRO = {"A": (1.12, 0.006), "B": (0.96, -0.022), "C": (1.0, -0.010), "D": (0.98, 0.010), "E": (1.04, 0.004),
            "F": (1.10, -0.004), "G": (1.18, -0.015), "H": (1.0, -0.008)}   # (niveau, tendance) relatifs, démo
EUROPE = ["DE", "GB", "IT", "ES", "NL", "CH", "SE", "BE", "DK", "AT"]
FR_SPE = {"A.01": 1.5, "A.02": 1.3, "A.03": 1.0, "A.04": 0.8, "B.01": 0.9, "B.02": 0.8, "B.03": 0.9,
          "B.04": 1.2, "B.05": 1.1, "B.06": 1.4, "B.07": 1.2, "C.01": 0.9, "C.02": 1.2, "C.03": 1.1,
          "C.04": 1.1, "C.05": 0.9, "C.06": 1.2, "C.07": 1.1, "C.08": 1.2, "C.09": 0.9, "C.10": 0.7,
          "C.11": 0.9, "D.01": 0.7, "D.02": 0.6, "D.03": 0.7, "D.04": 0.8, "D.05": 1.1, "D.06": 1.4,
          "D.07": 0.9, "E.01": 1.2, "E.02": 0.9, "E.03": 0.8, "E.04": 1.0, "E.05": 1.1, "E.06": 1.3,
          "F.01": 1.9, "G.01": 1.6, "G.02": 1.6}
REGION_W = {"Île-de-France": 0.40, "Auvergne-Rhône-Alpes": 0.135, "Occitanie": 0.095,
            "Provence-Alpes-Côte d'Azur": 0.075, "Nouvelle-Aquitaine": 0.06, "Grand Est": 0.055,
            "Hauts-de-France": 0.045, "Bretagne": 0.04, "Pays de la Loire": 0.035, "Normandie": 0.022,
            "Bourgogne-Franche-Comté": 0.022, "Centre-Val de Loire": 0.02, "Corse": 0.0015,
            "Guadeloupe / Martinique": 0.003, "Guyane": 0.0015, "La Réunion": 0.004,
            "Collectivités d'outre-mer": 0.002}
NATIONAL_W = {"CNRS": 0.48, "Inserm": 0.17, "INRAE": 0.07, "CEA": 0.065, "Inria": 0.025, "IRD": 0.025,
              "Cirad": 0.012, "Ifremer": 0.008, "BRGM": 0.004, "ONERA": 0.004}
NATIONAL_SPE = {"CNRS": {"B": 1.4, "D": 1.2, "F": 1.6, "G": 1.6, "C": 0.8}, "Inserm": {"C": 2.2},
                "INRAE": {"A": 4.0}, "CEA": {"B": 2.0, "G": 2.5, "H": 1.6}, "Inria": {"E": 5.0, "F": 2.0},
                "IRD": {"A": 3.0, "D": 1.5}, "Cirad": {"A": 5.0}, "Ifremer": {"A": 5.0},
                "BRGM": {"A": 3.0, "G": 4.0}, "ONERA": {"B": 3.0, "E": 1.5}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(ROOT / "data" / "biblio_demo.json"))
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    rnd = random.Random(args.seed)
    params = json.loads((ROOT / "config" / "params.json").read_text())
    cfg = json.loads((ROOT / "config" / "axes.json").read_text())
    noms = {k: v for k, v in json.loads((ROOT / "config" / "pays.json").read_text()).items() if not k.startswith("_")}
    years = list(range(params["annee_debut"], params["annee_fin"] + 1))
    ny = len(years)
    cite_end = min(params.get("annee_fin_citations", years[-1]), years[-1])

    axes, seen = [], set()
    for a in cfg["axes"]:
        if a["code"] not in seen:
            seen.add(a["code"])
            axes.append(a)
    macro_of = {a["code"]: a["code"][0] for a in axes}
    disc_axes = [a["code"] for a in axes if macro_of[a["code"]] != "H"]
    h_axes = [a["code"] for a in axes if macro_of[a["code"]] == "H"]
    w_axis = {}
    for m, w in WORLD_W.items():
        members = [c for c in disc_axes if macro_of[c] == m]
        raw = {c: rnd.uniform(0.5, 1.5) for c in members}
        s = sum(raw.values())
        for c in members:
            w_axis[c] = w * raw[c] / s
    for c in h_axes:
        w_axis[c] = rnd.uniform(0.008, 0.04)
    world_p10 = 0.115          # part mondiale d'articles dans le top 10 % (> 10 % : articles plus cités que les autres types)
    world_pj = 0.020

    def gen(total0, growth, spe, impact, trend, rev=1.0, macro=None):
        """impact(c) : niveau relatif au monde ; trend : variation annuelle de l'impact."""
        cube = {}
        tot = [total0 * (1 + growth) ** i for i in range(ny)]
        for c in disc_axes + h_axes:
            drift = rnd.uniform(-0.02, 0.03)
            lvl, tr = impact(c), trend + rnd.gauss(0, 0.008)
            if macro:
                lvl *= macro[macro_of[c]][0]; tr += macro[macro_of[c]][1]
            n = [max(0, round(tot[i] * w_axis[c] * spe(c) * (1 + drift) ** i * rnd.uniform(0.96, 1.04))) for i in range(ny)]
            t10, tj = [], []
            for i, x in enumerate(n):
                f = lvl * (1 + tr) ** i
                p10 = min(0.6, world_p10 * f)
                if years[i] > cite_end:              # année récente : citations encore incomplètes
                    p10 *= 0.6
                t10.append(max(0, round(x * p10 * rnd.uniform(0.92, 1.08))))
                tj.append(max(0, round(x * world_pj * (f ** 1.3) * rev * rnd.uniform(0.85, 1.15))))
            cube[c] = [n, t10, tj]
        for m in list(WORLD_W) + ["H"]:
            members = [c for c in disc_axes + h_axes if macro_of[c] == m]
            k = 0.75 if m == "H" else 0.97
            cube[m] = [[round(sum(cube[c][j][i] for c in members) * k) for i in range(ny)] for j in range(3)]
        cube["ALL"] = [[round(sum(cube[m][j][i] for m in WORLD_W) * 1.06) for i in range(ny)] for j in range(3)]
        return cube

    world_tot = 4_100_000
    ents, cube = [], {}
    ents.append({"id": "WORLD", "type": "world", "label": "Monde"})
    cube["WORLD"] = gen(world_tot, 0.035, lambda c: 1.0, lambda c: 1.0, 0.0)
    # spécialisations disciplinaires aléatoires mais stables par pays
    def country_spe(code):
        r = random.Random(code)
        return {c: math.exp(r.gauss(0, 0.35)) for c in disc_axes + h_axes}
    fr = FR_PARAMS
    ents.append({"id": "FR", "type": "nation", "label": "France"})
    cube["FR"] = gen(world_tot * fr[0], fr[1], lambda c: FR_SPE.get(c, 1.0),
                     lambda c: fr[2] * math.exp(rnd.gauss(0, 0.10)), fr[3], macro=FR_MACRO)
    world_panel = sorted(list(COUNTRIES) + ["FR"], key=lambda c: -(COUNTRIES.get(c, fr)[0]))[:20]
    for code, (sh, g, lvl, tr) in COUNTRIES.items():
        sp = country_spe(code)
        ents.append({"id": "C:" + code, "type": "country", "code": code, "label": noms.get(code, code),
                     "europe": code in EUROPE, "monde": code in world_panel})
        cube["C:" + code] = gen(world_tot * sh, g, lambda c, sp=sp: sp[c],
                                lambda c, lvl=lvl: lvl * math.exp(rnd.gauss(0, 0.12)), tr)

    rows = list(csv.DictReader((ROOT / "config" / "etablissements.csv").open(encoding="utf-8"), delimiter=";"))
    fr_tot = world_tot * fr[0]
    for reg, w in REGION_W.items():
        rid = "R:" + reg
        ents.append({"id": rid, "type": "region", "label": reg})
        rs = {c: math.exp(rnd.gauss(0, 0.3)) for c in disc_axes + h_axes}
        ri = rnd.uniform(0.88, 1.12) * (1.06 if reg == "Île-de-France" else 1)
        cube[rid] = gen(fr_tot * w * 1.15, rnd.uniform(-0.005, 0.02), lambda c, rs=rs: FR_SPE.get(c, 1) * rs[c],
                        lambda c, ri=ri: fr[2] * ri * math.exp(rnd.gauss(0, 0.12)), fr[3] + rnd.gauss(0, 0.01))
    by_reg = {}
    for r in rows:
        by_reg.setdefault(r["region"], []).append(r)
    for i, r in enumerate(rows):
        eid = (r.get("openalex_id") or "").strip() or f"DEMO{i:03d}"
        ents.append({"id": eid, "type": "etab", "label": r["nom"], "region": r["region"], "etab_type": r["type"]})
        if r["region"] in REGION_W:
            peers = by_reg[r["region"]]
            k = peers.index(r)
            share = (0.55 if k == 0 else 0.35 / (k + 1)) if len(peers) > 1 else 0.9
            if r["type"] in ("CHU", "CLCC"):
                share *= 0.6
            tot = fr_tot * REGION_W[r["region"]] * share
            spe_m = {"CHU": {"C": 2.6, "H": 1.3}, "CLCC": {"C": 3.0}, "Fondation": {"C": 2.8},
                     "École": {"B": 2.0, "E": 2.0}}.get(r["type"], {})
        else:
            tot = fr_tot * NATIONAL_W.get(r["nom"], 0.01)
            spe_m = NATIONAL_SPE.get(r["nom"], {})
        es = {c: math.exp(rnd.gauss(0, 0.35)) for c in disc_axes + h_axes}
        ei = rnd.uniform(0.82, 1.22)
        cube[eid] = gen(tot, rnd.uniform(-0.01, 0.025),
                        lambda c, es=es, sm=spe_m: FR_SPE.get(c, 1) * es[c] * sm.get(c[0], 0.5 if sm else 1),
                        lambda c, ei=ei: fr[2] * ei * math.exp(rnd.gauss(0, 0.15)), fr[3] + rnd.gauss(0, 0.012))

    out = {
        "meta": {"generated": date.today().isoformat(), "demo": True,
                 "source": "DONNÉES SYNTHÉTIQUES DE DÉMONSTRATION — aucune valeur réelle",
                 "years": years, "types": params["types_documents"],
                 "top10_definition": "citation_normalized_percentile.is_in_top_10_percent (OpenAlex)",
                 "citation_end_year": cite_end, "max_auteurs": params.get("max_auteurs"),
                 "panels": {"europe": ["C:" + c for c in EUROPE],
                            "monde": ["FR" if c == "FR" else "C:" + c for c in world_panel],
                            "monde_annees": [years[-3], years[-1]]}},
        "taxonomy": {
            "macros": [{"code": m["code"], "label": m["label"], "short": m["short"], "transverse": bool(m.get("transverse"))}
                       for m in cfg["macros"]],
            "axes": [{"code": a["code"], "label": a["label"], "macro": a["code"][0], "group": a.get("group")} for a in axes],
        },
        "entities": ents, "topj": {}, "cube": cube,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    print("écrit", args.out, f"({Path(args.out).stat().st_size/1e6:.1f} Mo, {len(ents)} entités)")


if __name__ == "__main__":
    main()
