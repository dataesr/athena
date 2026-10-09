"""Classement SJR (Scimago Journal Rank) → top N revues par discipline.

Source : le fichier global exporté depuis https://www.scimagojr.com/journalrank.php
(bouton « Download data », toutes catégories). Scimago bloque les téléchargements
automatisés (HTTP 403) : on dépose ce fichier à la main dans data/sjr/, par exemple
data/sjr/scimagojr 2025.csv, ou on le désigne avec --sjr-fichier.

La colonne « Categories » donne, pour chaque revue, ses catégories ASJC par leur NOM
(ex. « Oncology (Q1); Hematology (Q1) »). Les sous-domaines OpenAlex portant les mêmes
intitulés ASJC, on rattache chaque nom au code du sous-domaine correspondant. Les noms
non rattachés sont listés dans data/sjr/categories_non_rattachees.csv ; on peut les
rattacher à la main dans config/sjr_alias.json ({"Nom SJR": code ASJC}).
"""
from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
from collections import Counter
from pathlib import Path

# catégories ASJC « générales » : pas de sous-domaine OpenAlex équivalent
GENERAL_RE = re.compile(r"\((miscellaneous|general)\)$|^multidisciplinary$", re.I)
QUARTILE_RE = re.compile(r"\s*\(Q[1-4]\)\s*$")

# intitulés SJR qui diffèrent d'un sous-domaine OpenAlex (correspondances ASJC connues)
BUILTIN_ALIAS = {
    "biochemistry (medical)": 2704,
    "genetics (clinical)": 2716,
    "microbiology (medical)": 2726,
    "physiology (medical)": 2737,
    "pharmacology (medical)": 2736,
    "neurology (clinical)": 2728,
    "immunology and allergy": 2723,
    "pathology and forensic medicine": 2734,
    "pharmacology, toxicology and pharmaceutics (miscellaneous)": None,
}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = s.replace("&", " and ")
    return re.sub(r"[^a-z0-9()]+", " ", s).strip()


def _fmt_issn(raw: str) -> str | None:
    raw = raw.strip().replace("-", "").upper()
    if len(raw) != 8:
        return None
    return raw[:4] + "-" + raw[4:]


class SjrIndex:
    """Classement SJR global indexé par code ASJC."""

    def __init__(self, path: Path, types: list[str], subfield_names: dict[int, str], alias_file: Path | None = None):
        self.path = path
        text = path.read_text(encoding="utf-8-sig")
        header = text.split("\n", 1)[0]
        m = re.search(r"Total Docs\. \((\d{4})\)", header) or re.search(r"(\d{4})", path.name)
        self.year = int(m.group(1)) if m else None

        name_to_code = {_norm(n): c for c, n in subfield_names.items() if n}
        for k, v in BUILTIN_ALIAS.items():
            name_to_code.setdefault(_norm(k), v)
        if alias_file and alias_file.exists():
            for k, v in json.loads(alias_file.read_text(encoding="utf-8")).items():
                if not k.startswith("_"):
                    name_to_code[_norm(k)] = v

        self.all: list[dict] = []
        self.by_code: dict[int, list[dict]] = {}
        self.unmatched: Counter = Counter()
        self.general: Counter = Counter()
        for r in csv.DictReader(io.StringIO(text), delimiter=";"):
            t = (r.get("Type") or "").strip().lower()
            if types and t not in types:
                continue
            try:
                score = float((r.get("SJR") or "").replace(",", "."))
            except ValueError:
                continue
            issns = [i for i in (_fmt_issn(x) for x in (r.get("Issn") or "").split(",")) if i]
            if not issns:
                continue
            cats = [QUARTILE_RE.sub("", c).strip() for c in (r.get("Categories") or "").split(";") if c.strip()]
            j = {"title": (r.get("Title") or "").strip(), "issns": issns, "sjr": score,
                 "sourceid": r.get("Sourceid"), "categories": cats}
            self.all.append(j)
            for c in cats:
                code = name_to_code.get(_norm(c), "?")
                if code == "?":
                    if GENERAL_RE.search(c.strip()):
                        self.general[c] += 1
                    else:
                        self.unmatched[c] += 1
                elif code is not None:
                    self.by_code.setdefault(int(code), []).append(j)
        self.all.sort(key=lambda j: -j["sjr"])

    def top(self, categories: set[int] | None, n: int) -> list[dict]:
        """Top n revues (SJR décroissant) sur l'union des catégories ; None = toutes catégories."""
        if categories is None:
            return self.all[:n]
        pool = {}
        for c in categories:
            for j in self.by_code.get(int(c), []):
                pool[j["sourceid"] or j["issns"][0]] = j
        return sorted(pool.values(), key=lambda j: -j["sjr"])[:n]

    def write_report(self, out: Path) -> None:
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["categorie_sjr", "nb_revues", "statut"])
            for c, k in self.unmatched.most_common():
                w.writerow([c, k, "non rattachée — à ajouter dans config/sjr_alias.json"])
            for c, k in self.general.most_common():
                w.writerow([c, k, "catégorie générale (sans sous-domaine OpenAlex) — ignorée"])


def find_file(sjr_dir: Path, explicit: str | None = None) -> Path | None:
    if explicit:
        p = Path(explicit).expanduser()
        if not p.exists():
            raise FileNotFoundError(f"Fichier SJR introuvable : {p}")
        return p
    cands = list(sjr_dir.glob("scimagojr*.csv")) + list(sjr_dir.glob("sjr_*_all.csv"))
    if not cands:
        return None

    def year_of(p: Path) -> int:
        m = re.search(r"(\d{4})", p.name)
        return int(m.group(1)) if m else 0
    return max(cands, key=lambda p: (year_of(p), p.stat().st_mtime))


MISSING_MSG = """
  Fichier SJR absent. Scimago bloque les téléchargements automatisés : il faut l'exporter à la main.
    1. Ouvrir https://www.scimagojr.com/journalrank.php (toutes catégories, type « Journals »)
    2. Cliquer sur « Download data » (fichier « scimagojr AAAA.csv »)
    3. Le déposer dans {dir}/  — ou lancer le script avec --sjr-fichier "chemin/vers/scimagojr AAAA.csv"
"""
