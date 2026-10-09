"""Politique de fraîcheur du cache (data/cache/).

Le cache n'est jamais vidé brutalement : un rafraîchissement enregistre une date
d'invalidation par périmètre (data/cache/invalidation.json). Toute entrée plus
ancienne que cette date est considérée comme périmée et re-téléchargée au prochain
passage ; tant qu'elle ne l'a pas été, l'ancienne réponse reste sur disque.
Conséquences :
  - un rafraîchissement interrompu reprend où il s'était arrêté (relancer SANS --refresh) ;
  - si un téléchargement échoue (ex. SJR), l'ancienne version peut servir de repli.

Périmètres :
  comptages       requêtes /works (publications, top 10 %, top revues) — le gros du volume
  referentiel     liste des topics OpenAlex (rattachement aux axes)
  etablissements  résolution des noms d'établissements en identifiants OpenAlex
  sjr             classements Scimago
"""
from __future__ import annotations

import json
import time
from pathlib import Path

SCOPES = ("comptages", "referentiel", "etablissements", "sjr")
PATH_SCOPE = {"/works": "comptages", "/topics": "referentiel", "/institutions": "etablissements",
              "/sources": "sjr"}  # identification des revues SJR dans OpenAlex


class CachePolicy:
    def __init__(self, cache_root: Path, max_age_days: float | None = None):
        self.root = Path(cache_root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.file = self.root / "invalidation.json"
        self.marks: dict[str, float] = json.loads(self.file.read_text()) if self.file.exists() else {}
        self.max_age_s = max_age_days * 86400 if max_age_days else None

    def invalidate(self, scopes: list[str]) -> None:
        now = time.time()
        for s in scopes:
            self.marks[s] = now
        self.file.write_text(json.dumps(self.marks, indent=2))

    def is_fresh(self, path: Path, scope: str) -> bool:
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            return False
        if mtime < self.marks.get(scope, 0):
            return False
        if self.max_age_s and time.time() - mtime > self.max_age_s:
            return False
        return True

    def describe(self) -> str:
        if not self.marks:
            return "aucun rafraîchissement demandé"
        return ", ".join(f"{k} depuis le {time.strftime('%d/%m/%Y %H:%M', time.localtime(v))}"
                         for k, v in sorted(self.marks.items()))
