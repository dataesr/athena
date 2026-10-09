"""Référentiel disciplinaire : axes ↔ sous-domaines / topics OpenAlex.

OpenAlex classe chaque publication dans un « topic principal » (~4 500 topics),
lui-même rattaché à un sous-domaine (252 sous-domaines = codes ASJC, les mêmes
que les catégories SJR/Scimago). Chaque axe est défini comme un ensemble de
sous-domaines + un ensemble de topics supplémentaires (mots-clés).

Comme le topic principal est unique par publication, les fragments de filtre
(paquets de sous-domaines / paquets de topics hors de ces sous-domaines) sont
disjoints : leurs comptages s'additionnent sans double compte.
"""
from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

OR_LIMIT = 100  # nombre max de valeurs OU dans un filtre OpenAlex


@dataclass
class Discipline:
    code: str
    label: str
    level: str                     # "all" | "macro" | "axis"
    macro: str | None = None
    group: str | None = None
    transverse: bool = False
    subfields: set[int] = field(default_factory=set)
    extra_topics: set[str] = field(default_factory=set)
    sjr_categories: set[int] = field(default_factory=set)

    def fragments(self) -> list[str]:
        """Fragments de filtre disjoints dont la somme des comptages = la discipline."""
        if self.level == "all":
            return [""]
        frags = []
        sf = sorted(self.subfields)
        for i in range(0, len(sf), OR_LIMIT):
            frags.append("primary_topic.subfield.id:" + "|".join(str(s) for s in sf[i:i + OR_LIMIT]))
        tp = sorted(self.extra_topics)
        for i in range(0, len(tp), OR_LIMIT):
            frags.append("primary_topic.id:" + "|".join(tp[i:i + OR_LIMIT]))
        return frags


def _kw_text(k) -> str:
    """Mot-clé de topic : texte simple ou objet OpenAlex {"id", "display_name"}."""
    if isinstance(k, str):
        return k
    if isinstance(k, dict):
        return str(k.get("display_name") or k.get("keyword") or k.get("name") or "")
    return str(k or "")


def short_id(url: str) -> str:
    return url.rstrip("/").split("/")[-1]


def fetch_topics(oa, out_path: Path, policy=None) -> list[dict]:
    if out_path.exists() and (policy is None or policy.is_fresh(out_path, "referentiel")):
        return json.loads(out_path.read_text())
    topics = []
    for t in oa.iter_cursor("/topics", {"select": "id,display_name,keywords,subfield,field,domain"}):
        topics.append({
            "id": short_id(t["id"]),
            "name": t["display_name"],
            "keywords": [_kw_text(k) for k in (t.get("keywords") or [])],
            "subfield": int(short_id(t["subfield"]["id"])),
            "subfield_name": t["subfield"]["display_name"],
            "field": t["field"]["display_name"],
        })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(topics, ensure_ascii=False))
    return topics


def build(axes_cfg: dict, topics: list[dict], kw_share: float = 0.10) -> tuple[list[Discipline], dict]:
    all_sub = {t["subfield"] for t in topics}
    macros = {m["code"]: m for m in axes_cfg["macros"]}
    disc: list[Discipline] = [Discipline("ALL", "Toutes disciplines", "all")]
    axes: list[Discipline] = []
    seen = set()
    for a in axes_cfg["axes"]:
        if a["code"] in seen:
            continue  # doublon éventuel dans la liste source (ex. E.03 répété)
        seen.add(a["code"])
        m = a["code"].split(".")[0]
        axes.append(Discipline(a["code"], a["label"], "axis", macro=m, group=a.get("group"),
                               transverse=bool(macros[m].get("transverse")),
                               subfields=set(a.get("subfields", [])) & all_sub))
    by_code = {d.code: d for d in axes}

    # sous-domaines « résiduels » (catchall) des axes disciplinaires
    claimed = set().union(*(d.subfields for d in axes if not d.transverse))
    for a in axes_cfg["axes"]:
        d = by_code[a["code"]]
        for p in a.get("catchall_prefixes", []):
            for s in sorted(all_sub):
                if str(s).startswith(p) and s not in claimed:
                    d.subfields.add(s)
                    claimed.add(s)

    # topics ajoutés par mots-clés
    kw_report = {}
    for a in axes_cfg["axes"]:
        d = by_code[a["code"]]
        kws = [k.lower() for k in a.get("topic_keywords", [])]
        if not kws:
            continue
        matched = []
        for t in topics:
            hay = (str(t["name"]) + " | " + " | ".join(_kw_text(k) for k in t["keywords"])).lower()
            if any(k in hay for k in kws):
                matched.append(t)
        kw_report[d.code] = matched
        d.extra_topics = {t["id"] for t in matched if t["subfield"] not in d.subfields}
        cnt = Counter(t["subfield"] for t in matched)
        tot = sum(cnt.values()) or 1
        d.sjr_categories |= {s for s, c in cnt.items() if c / tot >= kw_share}
    for d in axes:
        d.sjr_categories |= d.subfields

    # macro-disciplines = union de leurs axes
    for code, m in macros.items():
        members = [d for d in axes if d.macro == code]
        md = Discipline(code, m["label"], "macro", transverse=bool(m.get("transverse")))
        md.subfields = set().union(*(d.subfields for d in members))
        md.extra_topics = {t for d in members for t in d.extra_topics}
        topic_sub = {t["id"]: t["subfield"] for t in topics}
        md.extra_topics = {t for t in md.extra_topics if topic_sub[t] not in md.subfields}
        md.sjr_categories = set().union(*(d.sjr_categories for d in members))
        disc.append(md)
    disc.extend(axes)

    report = {
        "unassigned_subfields": sorted(all_sub - claimed),
        "keyword_matches": {k: [t["id"] for t in v] for k, v in kw_report.items()},
    }
    return disc, report


def write_review_csv(disc: list[Discipline], topics: list[dict], path: Path):
    """Tableau de revue experte : un topic par ligne, avec les axes qui le contiennent."""
    axes = [d for d in disc if d.level == "axis"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["topic_id", "topic", "sous_domaine_asjc", "sous_domaine", "domaine", "axes_disciplinaires",
                    "dont_par_mots_cles", "axes_transverses"])
        for t in topics:
            hit = [d for d in axes if t["subfield"] in d.subfields or t["id"] in d.extra_topics]
            w.writerow([t["id"], t["name"], t["subfield"], t["subfield_name"], t["field"],
                        ",".join(d.code for d in hit if not d.transverse),
                        ",".join(d.code for d in hit if not d.transverse and t["id"] in d.extra_topics),
                        ",".join(d.code for d in hit if d.transverse)])


def dashboard_topics(disc: list[Discipline], topics: list[dict]) -> dict:
    """Rattachement topics → axes (38 disciplinaires, 19 transverses), pour la transparence dans le tableau de bord.

    by_axis[axe] = [[topic_id, nom, code ASJC, via]] (axes disciplinaires et transverses) où via = "sd" (le sous-domaine du topic
    appartient à l'axe) ou "mc" (topic ajouté par mot-clé, hors des sous-domaines de l'axe).
    """
    axes = [d for d in disc if d.level == "axis"]   # 38 disciplinaires + 19 transverses
    by_axis = {d.code: [] for d in axes}
    unassigned = []   # topics rattachés à aucun axe DISCIPLINAIRE
    for t in sorted(topics, key=lambda t: (t["subfield"], t["name"])):
        hit = False
        for d in axes:
            via = "sd" if t["subfield"] in d.subfields else "mc" if t["id"] in d.extra_topics else None
            if via:
                by_axis[d.code].append([t["id"], t["name"], t["subfield"], via])
                hit = hit or not d.transverse
        if not hit:
            unassigned.append([t["id"], t["name"], t["subfield"], ""])
    return {"subfields": {str(t["subfield"]): t["subfield_name"] for t in topics},
            "by_axis": by_axis, "unassigned": unassigned, "n_topics": len(topics)}
