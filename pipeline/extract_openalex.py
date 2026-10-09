#!/usr/bin/env python3
"""Extraction bibliométrique OpenAlex → data/biblio.json

Indicateurs (par entité × discipline × année de publication) :
  n    nombre de publications (articles + revues de synthèse) avec ≥ 1 affiliation de l'entité
  t10  dont publications dans le top 10 % des plus citées (centile normalisé OpenAlex :
       même année de publication, même sous-domaine)
  tj   dont publications parues dans l'une des 10 « meilleures » revues SJR de la discipline

Entités : Monde, France, pays de comparaison (panel Europe + panel monde), régions,
établissements (config/etablissements.csv).
Disciplines : Toutes disciplines, 8 macro-disciplines, 56 axes (config/axes.json).

Usage :
  export OPENALEX_API_KEY=xxxxxxxx        # clé API OpenAlex
  export OPENALEX_MAILTO=prenom.nom@...    # facultatif
  python pipeline/extract_openalex.py --dry-run             # estime le nombre de requêtes
  python pipeline/extract_openalex.py                       # extraction complète
  python pipeline/extract_openalex.py --max-calls 20000     # par tranches (reprise via le cache)
  python pipeline/extract_openalex.py --refresh             # tout re-télécharger (cache conservé jusqu'au remplacement)
  python pipeline/extract_openalex.py --refresh comptages   # seulement les comptages (garde référentiel, SJR…)
  python pipeline/extract_openalex.py --max-age-jours 30    # re-télécharge ce qui a plus de 30 jours
  python pipeline/extract_openalex.py --cache-info          # état du cache
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import referentiel  # noqa: E402
import sjr  # noqa: E402
from cache_policy import SCOPES, CachePolicy  # noqa: E402
from openalex import BudgetExceeded, OpenAlex, QueryTooComplex  # noqa: E402

# champs à valeur unique par publication : un OU sur ces champs peut être coupé en deux
# et les comptages additionnés sans double compte (ce n'est PAS le cas des ISSN — une revue
# en a plusieurs — ni des établissements — une publication en a plusieurs).
SPLITTABLE = ("primary_topic.id:", "primary_topic.subfield.id:")


def count_split(oa: OpenAlex, filters: list[str], depth: int = 0) -> dict[int, int]:
    """Comptage par année ; si OpenAlex juge la requête trop lourde, coupe le plus grand
    OU « découpable » en deux, interroge chaque moitié et additionne."""
    try:
        return oa.count_by_year(filters)
    except QueryTooComplex:
        cands = [(i, f) for i, f in enumerate(filters) if f.startswith(SPLITTABLE) and "|" in f]
        if not cands or depth > 8:
            raise
        i, f = max(cands, key=lambda x: x[1].count("|"))
        key, vals = f.split(":", 1)
        vals = vals.split("|")
        half = len(vals) // 2
        out: dict[int, int] = {}
        for part in (vals[:half], vals[half:]):
            sub = filters[:i] + [key + ":" + "|".join(part)] + filters[i + 1:]
            for y, c in count_split(oa, sub, depth + 1).items():
                out[y] = out.get(y, 0) + c
        oa.store_count_by_year(filters, out)  # les relances liront directement le total
        return out

ROOT = Path(__file__).resolve().parent.parent
NATIONAL = "National (multi-sites)"


def _name_sim(a: str, b: str) -> float:
    import difflib
    import unicodedata

    def norm(x):
        x = unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode().lower()
        return " ".join(re.sub(r"[^a-z0-9]+", " ", x).split())
    return difflib.SequenceMatcher(None, norm(a), norm(b)).ratio()


SUB_UNIT = re.compile(r"\b(laborato|labo\b|medialab|institut de recherche|unit[eé]|umr\b|centre de recherche|"
                      r"department|d[ée]partement|faculty|facult[ée]|school of|[ée]cole doctorale)", re.I)


def pick_institution(query: str, results: list[dict]) -> tuple[dict, str]:
    """Choisit la fiche la plus plausible : nom proche de la recherche, pas une sous-unité
    (laboratoire, département…), et la plus grosse production. Renvoie (fiche, alerte)."""
    scored = []
    for r in results:
        sim = _name_sim(query, r.get("display_name", ""))
        sub = bool(SUB_UNIT.search(r.get("display_name", ""))) and not SUB_UNIT.search(query)
        scored.append((sim >= 0.6 and not sub, sim >= 0.9, r.get("works_count") or 0, sim, r))
    # priorité : pas une sous-unité, nom quasi identique, puis plus grosse production
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    ok, _, works, sim, best = scored[0]
    flags = []
    if not ok:
        flags.append("nom éloigné de la recherche ou sous-unité")
    if works < 2000:
        flags.append(f"peu de publications ({works})")
    if best["id"].split("/")[-1].startswith("I44"):
        flags.append("fiche OpenAlex récente")
    return best, " ; ".join(flags)


def load_etabs(oa: OpenAlex, path: Path, out_path: Path, limit: int | None, countries: list[str]) -> list[dict]:
    """Résout les identifiants OpenAlex des établissements (recherche par nom) et écrit
    un fichier de contrôle data/etablissements_resolus.csv à vérifier une fois.
    Un identifiant renseigné dans la colonne openalex_id de config/etablissements.csv
    est toujours prioritaire sur la recherche automatique."""
    rows = list(csv.DictReader(path.open(encoding="utf-8"), delimiter=";"))
    if limit:
        rows = rows[:limit]
    resolved = []
    n_flag = 0
    for r in rows:
        oid = (r.get("openalex_id") or "").strip()
        found, works, flag, source = "", "", "", "manuel"
        if not oid:
            source = "recherche"
            data = oa.get("/institutions", {"search": r["recherche_openalex"],
                                             "filter": "country_code:" + "|".join(c.lower() for c in countries),
                                             "per_page": 10, "select": "id,display_name,works_count"})
            res = data.get("results", [])
            if not res:
                print(f"  ! établissement introuvable dans OpenAlex : {r['nom']}")
                continue
            best, flag = pick_institution(r["recherche_openalex"], res)
            oid, found, works = best["id"].split("/")[-1], best["display_name"], best.get("works_count", "")
            if flag:
                n_flag += 1
                print(f"  ⚠ à vérifier : {r['nom']} → {found} ({oid}) — {flag}")
        resolved.append({"id": oid, "label": r["nom"], "region": r["region"], "type": r["type"],
                         "openalex_name": found, "works": works, "flag": flag, "source": source})
    if n_flag:
        print(f"    {n_flag} correspondance(s) à vérifier : python3 pipeline/chercher_etablissement.py \"Nom\" "
              "puis renseigner openalex_id dans config/etablissements.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["nom", "region", "type", "openalex_id", "nom_openalex_trouve", "nb_publications_openalex",
                    "source", "a_verifier"])
        for e in resolved:
            w.writerow([e["label"], e["region"], e["type"], e["id"], e["openalex_name"], e["works"],
                        e["source"], e["flag"]])
    return resolved


def resolve_sources(oa: OpenAlex, topj: dict[str, list[dict]]) -> int:
    """Associe chaque revue SJR à sa fiche OpenAlex (recherche par ISSN, 100 ISSN par requête).
    Ajoute openalex_id / openalex_name ; renvoie le nombre de revues non trouvées."""
    all_issns = sorted({i for lst in topj.values() for j in lst for i in j["issns"]})
    by_issn: dict[str, dict] = {}
    for k in range(0, len(all_issns), referentiel.OR_LIMIT):
        chunk = all_issns[k:k + referentiel.OR_LIMIT]
        data = oa.get("/sources", {"filter": "issn:" + "|".join(chunk), "per_page": 100,
                                   "select": "id,display_name,issn_l,issn,works_count"})
        for src in data.get("results", []):
            for i in (src.get("issn") or []) + [src.get("issn_l")]:
                if i:
                    by_issn.setdefault(i.upper(), src)
    missing = 0
    for lst in topj.values():
        for j in lst:
            src = next((by_issn[i] for i in j["issns"] if i in by_issn), None)
            j["openalex_id"] = src["id"].split("/")[-1] if src else None
            j["openalex_name"] = src["display_name"] if src else None
            missing += src is None
    return missing


def country_code(key: str) -> str:
    return str(key).rstrip("/").split("/")[-1].upper()


def world_panel(oa: OpenAlex, params: dict, cfg_dir: Path, recompute: bool) -> dict:
    """Panel monde : les N premiers pays producteurs sur les dernières années de la période
    (méthode OST), calculé une fois puis figé dans config/panel_monde.json."""
    path = cfg_dir / "panel_monde.json"
    if path.exists() and not recompute:
        return json.loads(path.read_text(encoding="utf-8"))
    n = params.get("panel_monde_taille", 20)
    k = params.get("panel_monde_annees", 3)
    ya, yb = params["annee_fin"] - k + 1, params["annee_fin"]
    filt = [f"publication_year:{ya}-{yb}", "type:" + "|".join(params["types_documents"])]
    if params.get("max_auteurs"):
        filt.append(f"authors_count:<{int(params['max_auteurs']) + 1}")
    data = oa.get("/works", {"filter": ",".join(filt), "group_by": "authorships.countries"})
    fr_codes = set(params.get("codes_pays_france", ["FR"]))
    counts: dict[str, int] = {}
    for g in data.get("group_by", []):
        c = country_code(g.get("key", ""))
        if not c or c in ("UNKNOWN", "NULL"):
            continue
        c = "FR" if c in fr_codes else c
        counts[c] = counts.get(c, 0) + int(g.get("count", 0))
    top = sorted(counts.items(), key=lambda x: -x[1])[:n]
    panel = {"_doc": f"Panel monde figé le {date.today().isoformat()} : les {n} premiers pays producteurs "
                     f"d'articles ({ya}-{yb}, compte entier). Supprimer ce fichier pour le recalculer.",
             "annees": [ya, yb], "pays": [c for c, _ in top], "articles": {c: v for c, v in top}}
    path.write_text(json.dumps(panel, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"    Panel monde calculé et figé dans {path.name} : {', '.join(panel['pays'])}")
    return panel


def cache_info(data_dir: Path, policy: CachePolicy) -> None:
    import time as _t
    root = data_dir / "cache" / "openalex"
    fmt = lambda t: _t.strftime("%d/%m/%Y", _t.localtime(t))  # noqa: E731
    groups = {s: list((root / s).glob("*/*.json")) for s in ("comptages", "referentiel", "etablissements")}
    groups["sjr"] = list((data_dir / "sjr").glob("*.csv"))
    print("Périmètre        Entrées   Taille   Plus ancienne   Plus récente   Périmées")
    for s, files in groups.items():
        if not files:
            print(f"{s:<16}{0:>8}")
            continue
        mt = [f.stat().st_mtime for f in files]
        size = sum(f.stat().st_size for f in files) / 1e6
        stale = sum(1 for f in files if not policy.is_fresh(f, s))
        print(f"{s:<16}{len(files):>8}{size:>7.1f} Mo   {fmt(min(mt)):<16}{fmt(max(mt)):<15}{stale:>8}")
    print(f"Invalidations : {policy.describe()}.")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api-key", help="clé API OpenAlex (sinon OPENALEX_API_KEY)")
    ap.add_argument("--mailto", help="e-mail de contact (sinon OPENALEX_MAILTO)")
    ap.add_argument("--config", default=str(ROOT / "config"))
    ap.add_argument("--out", default=str(ROOT / "data" / "biblio.json"))
    ap.add_argument("--max-calls", type=int, help="nombre max de requêtes réseau pour cette exécution")
    ap.add_argument("--etabs-limit", type=int, help="ne traiter que les N premiers établissements")
    ap.add_argument("--workers", type=int, default=4, help="requêtes simultanées (défaut 4)")
    ap.add_argument("--rps", type=float, help="débit max en requêtes/s (défaut : params.json = 5, plafonné à 50)")
    ap.add_argument("--part-budget", type=float, default=0.5,
                    help="part maximale du budget OpenAlex restant du jour que l'extraction peut consommer "
                         "(défaut 0.5 : la moitié reste à vos autres applications)")
    ap.add_argument("--autoriser-prepaye", action="store_true",
                    help="autorise à consommer le solde prépayé OpenAlex une fois le budget gratuit du jour épuisé")
    ap.add_argument("--dry-run", action="store_true", help="estime le volume de requêtes et s'arrête")
    ap.add_argument("--refresh", nargs="*", choices=SCOPES + ("all",), metavar="PÉRIMÈTRE",
                    help="re-télécharge le cache : all (défaut), comptages, referentiel, etablissements, sjr. "
                         "En cas d'interruption, relancez SANS --refresh pour reprendre.")
    ap.add_argument("--max-age-jours", type=float, help="re-télécharge toute entrée du cache plus ancienne que N jours")
    ap.add_argument("--cache-info", action="store_true", help="affiche l'état du cache et s'arrête")
    ap.add_argument("--sjr-fichier", help="fichier SJR global exporté de scimagojr.com (sinon : data/sjr/scimagojr*.csv)")
    ap.add_argument("--recalculer-panel-monde", action="store_true",
                    help="recalcule le panel monde (top N producteurs) au lieu de relire config/panel_monde.json")
    args = ap.parse_args()

    cfg_dir = Path(args.config)
    params = json.loads((cfg_dir / "params.json").read_text())
    axes_cfg = json.loads((cfg_dir / "axes.json").read_text())
    data_dir = Path(args.out).parent
    policy = CachePolicy(data_dir / "cache", args.max_age_jours)
    if args.cache_info:
        cache_info(data_dir, policy)
        return
    if args.refresh is not None:
        scopes = list(SCOPES) if (not args.refresh or "all" in args.refresh) else args.refresh
        policy.invalidate(scopes)
        print(f"  ↻ Rafraîchissement demandé : {', '.join(scopes)}. Les anciennes réponses restent en cache "
              "jusqu'à leur remplacement ; si l'exécution s'interrompt, relancez sans --refresh.")
    oa = OpenAlex(args.api_key, args.mailto, data_dir / "cache" / "openalex",
                  rps=args.rps or params.get("requetes_par_seconde", 25), max_calls=args.max_calls,
                  allow_prepaid=args.autoriser_prepaye, budget_share=args.part_budget,
                  pool_size=max(8, args.workers), policy=policy,
                  concurrency=args.workers)
    if not oa.api_key:
        print("  ⚠ Aucune clé API OpenAlex (OPENALEX_API_KEY) : budget réduit à 0,10 $/jour (≈ 1 000 requêtes).")

    y0, y1 = params["annee_debut"], params["annee_fin"]
    years = list(range(y0, y1 + 1))
    base = [f"publication_year:{y0}-{y1}", "type:" + "|".join(params["types_documents"])]
    max_aut = params.get("max_auteurs")
    if max_aut:
        base.append(f"authors_count:<{int(max_aut) + 1}")
        print(f"  ℹ Articles de plus de {int(max_aut)} auteurs exclus de tous les comptages (paramètre max_auteurs).")
    cite_end = min(int(params.get("annee_fin_citations", y1)), y1)

    # 1. Référentiel disciplinaire
    print("1/5 Référentiel disciplinaire (topics OpenAlex)…")
    topics = referentiel.fetch_topics(oa, data_dir / "cache" / "topics.json", policy)
    discs, report = referentiel.build(axes_cfg, topics, params["seuil_sous_domaine_pour_axes_par_mots_cles"])
    referentiel.write_review_csv(discs, topics, data_dir / "referentiel_topics.csv")
    if report["unassigned_subfields"]:
        print(f"  ℹ sous-domaines sans axe disciplinaire : {report['unassigned_subfields']}")

    # 2. Entités
    print("2/5 Pays, établissements et régions…")
    pays_noms = {k: v for k, v in json.loads((cfg_dir / "pays.json").read_text(encoding="utf-8")).items()
                 if not k.startswith("_")}
    panel_eu = [c.upper() for c in params.get("panel_europe", [])]
    panel_w = world_panel(oa, params, cfg_dir, args.recalculer_panel_monde)
    panel_w_codes = [c for c in panel_w["pays"]]
    countries = [c for c in dict.fromkeys(panel_eu + panel_w_codes) if c != "FR"]
    print(f"    Panel Europe ({len(panel_eu)}) : {', '.join(panel_eu)}")
    print(f"    Panel monde ({len(panel_w_codes)}) : {', '.join(panel_w_codes)}"
          + ("" if "FR" in panel_w_codes else "  — la France n'y figure pas !"))
    etabs = load_etabs(oa, cfg_dir / "etablissements.csv", data_dir / "etablissements_resolus.csv", args.etabs_limit,
                       params.get("codes_pays_france", ["FR"]))
    regions: dict[str, list[str]] = {}
    for e in etabs:
        if e["region"] != NATIONAL:
            regions.setdefault(e["region"], []).append(e["id"])
    entities = []
    if params.get("inclure_monde", True):
        entities.append({"id": "WORLD", "type": "world", "label": "Monde", "filter": None})
    entities.append({"id": "FR", "type": "nation", "label": "France", "filter": params["filtre_france"]})
    for c in countries:
        entities.append({"id": "C:" + c, "type": "country", "code": c, "label": pays_noms.get(c, c),
                         "europe": c in panel_eu, "monde": c in panel_w_codes,
                         "filter": "authorships.countries:" + c})
    for reg, ids in sorted(regions.items()):
        if len(ids) > referentiel.OR_LIMIT:
            print(f"  ! {reg} : {len(ids)} établissements, seuls les {referentiel.OR_LIMIT} premiers sont retenus")
            ids = ids[:referentiel.OR_LIMIT]
        entities.append({"id": "R:" + reg, "type": "region", "label": reg,
                         "filter": "authorships.institutions.lineage:" + "|".join(ids)})
    for e in etabs:
        entities.append({"id": e["id"], "type": "etab", "label": e["label"], "region": e["region"],
                         "etab_type": e["type"], "filter": "authorships.institutions.lineage:" + e["id"]})

    # 3. Top revues SJR
    print("3/5 Top revues SJR…")
    sjr_dir = data_dir / "sjr"
    sjr_file = sjr.find_file(sjr_dir, args.sjr_fichier)
    if sjr_file is None:
        print(sjr.MISSING_MSG.format(dir=sjr_dir))
        sys.exit(1)
    subfield_names = {t["subfield"]: t["subfield_name"] for t in topics}
    idx = sjr.SjrIndex(sjr_file, params["sjr_types_retenus"], subfield_names, cfg_dir / "sjr_alias.json")
    idx.write_report(sjr_dir / "categories_non_rattachees.csv")
    print(f"    {sjr_file.name} : {len(idx.all)} revues, classement {idx.year or '?'}, "
          f"{len(idx.by_code)} catégories rattachées aux sous-domaines OpenAlex.")
    if idx.unmatched:
        print(f"  ℹ {len(idx.unmatched)} catégorie(s) SJR non rattachée(s) "
              f"(voir data/sjr/categories_non_rattachees.csv) : {', '.join(list(idx.unmatched)[:5])}…")
    # Revues de tête : 10 revues SJR pour chacun des axes (38 disciplinaires + 19 transverses).
    # Une macro-discipline ou « toutes disciplines » additionne les axes dans le tableau de bord :
    # pas de « top 10 global », qui n'aurait pas de sens d'une discipline à l'autre.
    is_disc_axis = lambda d: d.level == "axis"  # noqa: E731
    topj = {}
    for d in discs:
        if not is_disc_axis(d):
            topj[d.code] = []
            continue
        topj[d.code] = idx.top(d.sjr_categories, params["top_revues_n"])
        if not topj[d.code]:
            print(f"  ! aucune revue SJR pour {d.code} (catégories {sorted(d.sjr_categories)})")
    missing = resolve_sources(oa, topj)
    if missing:
        print(f"  ℹ {missing} revue(s) SJR sans correspondance OpenAlex par ISSN (signalées dans le tableau de bord).")
    topj_meta = {d.code: {"categories": [[c, subfield_names.get(c, "")] for c in sorted(d.sjr_categories)]}
                 for d in discs if is_disc_axis(d)}

    # 4. Requêtes
    tasks = []  # (entity_id, disc_code, metric_index, filter list)
    for ent in entities:
        ef = [ent["filter"]] if ent["filter"] else []
        for d in discs:
            issns = sorted({i for j in topj[d.code] for i in j["issns"]})
            for frag in d.fragments():
                ff = base + ef + ([frag] if frag else [])
                tasks.append((ent["id"], d.code, 0, ff))
                tasks.append((ent["id"], d.code, 1, ff + [params["filtre_top10"]]))
                if issns:
                    tasks.append((ent["id"], d.code, 2, ff + ["primary_location.source.issn:" + "|".join(issns)]))
    pending = sum(1 for t in tasks if not oa.is_cached("/works", oa.count_by_year_params(t[3])))
    sp = lambda n: f"{n:,}".replace(",", " ")  # noqa: E731
    print(f"4/5 {sp(len(tasks))} requêtes ({len(entities)} entités × {len(discs)} disciplines), "
          f"dont {sp(pending)} restant à faire (le reste est en cache).")
    b = oa.set_reserve()
    if b:
        print(f"    Budget OpenAlex disponible : {b['remaining_usd']:.2f} $ ≈ {sp(b['calls_left'])} requêtes"
              + ("" if args.autoriser_prepaye else " (budget du jour, hors solde prépayé)"))
        if pending:
            minutes = max(1, round(pending / oa.rps / 60))
            if int(b["calls_left"] * oa.budget_share) >= pending:
                print(f"    Budget suffisant : extraction complète en une passe, ≈ {minutes} min à {oa.rps:.0f} req/s.")
            else:
                print(f"    Part allouée insuffisante pour tout faire aujourd'hui : {sp(int(b['calls_left'] * oa.budget_share))} / {sp(pending)} requêtes ; "
                      "relancez après la remise à zéro (minuit UTC).")
        if pending and b["calls_left"] < 1:
            print("  ⏸", oa._budget_message())
            sys.exit(1)
        # on ne dépense jamais plus que la part allouée du budget restant
        allowed = int(b["calls_left"] * oa.budget_share)
        print(f"    Part allouée à cette extraction : {oa.budget_share:.0%} ≈ {sp(allowed)} requêtes "
              f"(réserve laissée aux autres applications : {oa.reserve_usd:.2f} $).")
        cap = oa.calls_network + max(0, allowed - 20)
        oa.max_calls = cap if oa.max_calls is None else min(oa.max_calls, cap)
    if args.dry_run:
        return

    cube = {e["id"]: {d.code: [[0] * len(years) for _ in range(3)] for d in discs} for e in entities}
    t0, done, failed = time.time(), 0, 0

    def run(t):
        return t, count_split(oa, t[3])

    stop = False
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(run, t) for t in tasks]
        for f in as_completed(futs):
            if f.cancelled():
                continue
            try:
                (eid, dcode, mi, _), by_year = f.result()
            except CancelledError:
                continue
            except BudgetExceeded as e:
                if not stop:
                    print("\n  ⏸", e)
                    stop = True
                    for g in futs:
                        g.cancel()
                continue
            except Exception as e:  # noqa: BLE001
                failed += 1
                print("\n  !", e)
                continue
            arr = cube[eid][dcode][mi]
            for y, c in by_year.items():
                if y0 <= y <= y1:
                    arr[y - y0] += c
            done += 1
            if done % 250 == 0:
                rate = oa.calls_network / max(1, time.time() - t0)
                print(f"\r  {done}/{len(tasks)} · réseau {oa.calls_network} ({rate:.1f} req/s) · cache {oa.calls_cache} · "
                      f"débit {oa.cur_rps:.0f}/s · simultanées {oa.conc_limit} · dépensé {oa.spent_usd:.3f} $", end="", flush=True)
    print()
    if oa.n_split:
        print(f"  ℹ {oa.n_split} requêtes jugées trop lourdes par OpenAlex, découpées en sous-requêtes.")
    if oa.n_throttled:
        print(f"  ℹ {oa.n_throttled} réponses 429 absorbées ; débit final {oa.cur_rps:.0f} req/s, "
              f"{oa.conc_limit} requêtes simultanées.")
    if stop or failed:
        again = "Relancez la commande SANS --refresh" if args.refresh is not None else "Relancez la même commande"
        print(f"  Extraction incomplète ({done}/{len(tasks)}). {again} pour terminer (le cache conserve l'avancement).")
        sys.exit(1)

    # 5. Export
    print("5/5 Écriture", args.out)
    out = {
        "meta": {
            "generated": date.today().isoformat(), "demo": False,
            "source": f"OpenAlex (api.openalex.org) ; classement SJR Scimago {idx.year or ''}".strip(),
            "years": years, "types": params["types_documents"],
            "top10_definition": "citation_normalized_percentile.is_in_top_10_percent (OpenAlex)",
            "citation_end_year": cite_end,
            "max_auteurs": max_aut,
            "panels": {"europe": ["C:" + c for c in panel_eu],
                       "monde": ["FR" if c == "FR" else "C:" + c for c in panel_w_codes],
                       "monde_annees": panel_w.get("annees")},
        },
        "taxonomy": {
            "macros": [{"code": m["code"], "label": m["label"], "short": m["short"],
                        "transverse": bool(m.get("transverse"))} for m in axes_cfg["macros"]],
            "axes": [{"code": d.code, "label": d.label, "macro": d.macro, "group": d.group,
                      "n_subfields": len(d.subfields), "n_extra_topics": len(d.extra_topics)}
                     for d in discs if d.level == "axis"],
        },
        "topics": referentiel.dashboard_topics(discs, topics),
        "entities": [{k: v for k, v in e.items() if k != "filter"} for e in entities],
        "topj": {k: [{"title": j["title"], "issn": ", ".join(j["issns"]), "sjr": j["sjr"],
                      "openalex_id": j.get("openalex_id"), "openalex_name": j.get("openalex_name")}
                     for j in v] for k, v in topj.items()},
        "topj_meta": {"sjr_year": idx.year, "sjr_file": sjr_file.name, "by_disc": topj_meta},
        "cube": cube,
    }
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    print(f"  ok — {oa.calls_network} requêtes réseau ({oa.spent_usd:.2f} $), dont {oa.calls_refreshed} rafraîchies ; "
          f"{oa.calls_cache} depuis le cache.")


if __name__ == "__main__":
    main()
