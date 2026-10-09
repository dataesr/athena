"""Client OpenAlex : clé API, parallélisme borné, budget quotidien, cache disque.

Règles OpenAlex (developers.openalex.org, authentification & tarifs) :
  - 100 requêtes/s maximum, au-delà : HTTP 429 ;
  - budget quotidien : 1 $/jour avec clé gratuite (≈ 10 000 requêtes liste+filtre à 0,0001 $),
    remis à zéro à minuit UTC ; au-delà, OpenAlex puise dans le solde PRÉPAYÉ s'il existe ;
  - le dépassement du budget renvoie AUSSI un 429 : on interroge /rate-limit pour distinguer.

Le budget est celui de la CLÉ : il est partagé avec les autres applications qui l'utilisent
(et avec openalex.org quand on y est connecté). Le script n'en consomme donc qu'une part.

Ce client :
  - limite le débit global (seau à jetons partagé par tous les threads, 5 req/s par défaut) ;
  - ne dépense qu'une fraction du budget restant du jour (50 % par défaut) et vérifie
    régulièrement que la réserve laissée aux autres applications est intacte ;
  - sur 429 « débit », met TOUS les threads en pause (recul exponentiel) puis reprend ;
  - sur 429 « budget épuisé », s'arrête proprement (BudgetExceeded) avec l'heure de reprise ;
  - plafonne la consommation au budget gratuit restant du jour, sauf autorisation explicite
    d'utiliser le solde prépayé (allow_prepaid=True) ;
  - met chaque réponse en cache (écriture atomique) : une extraction interrompue reprend
    sans re-consommer de requêtes.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter

BASE = "https://api.openalex.org"


def _is_too_complex(text: str) -> bool:
    t = text.lower()
    return "filter ors" in t or ("ors" in t and "values on" in t) or "too complex" in t
DEFAULT_LIST_COST = 0.0001  # $ par requête liste+filtre (repli si /rate-limit ne le précise pas)


class BudgetExceeded(RuntimeError):
    pass


class QueryTooComplex(RuntimeError):
    """OpenAlex refuse une requête trop lourde (trop de valeurs OU sur un champ très fréquent).
    Ce n'est pas un problème de débit : il faut découper le filtre, pas réessayer."""


class OpenAlex:
    def __init__(self, api_key: str | None = None, mailto: str | None = None,
                 cache_dir: str | Path = "data/cache/openalex", rps: float = 25.0,
                 max_calls: int | None = None, allow_prepaid: bool = False, budget_share: float = 0.5,
                 pool_size: int = 32, timeout: int = 60, policy=None, concurrency: int | None = None):
        self.api_key = api_key or os.environ.get("OPENALEX_API_KEY")
        self.mailto = mailto or os.environ.get("OPENALEX_MAILTO")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rps = min(max(rps, 0.5), 50.0)  # plafond dur 50 req/s (limite OpenAlex : 100, clé partagée)
        self.budget_share = min(max(budget_share, 0.01), 1.0)
        self.reserve_usd: float | None = None  # budget à laisser intact pour les autres usages de la clé
        self.max_calls = max_calls
        self.allow_prepaid = allow_prepaid
        self.timeout = timeout
        self.pool_size = pool_size
        self.policy = policy  # CachePolicy (fraîcheur du cache) ou None
        self._local = threading.local()
        # seau à jetons global
        self._lock = threading.Lock()
        self.cur_rps = self.rps          # débit courant, ajusté dynamiquement (AIMD)
        self._tokens = 1.0
        self._last_refill = time.monotonic()
        self._pause_until = 0.0
        self._last_429 = 0.0
        self._backoff = 2.0
        self.n_throttled = 0
        self.n_split = 0
        self._first_429_logged = False
        # nombre de requêtes simultanées, ajusté dynamiquement
        self.conc_max = max(1, concurrency or pool_size)
        self.conc_limit = self.conc_max
        self._active = 0
        self._ok_streak = 0
        self._cond = threading.Condition()
        # compteurs
        self.calls_network = 0
        self.calls_cache = 0
        self.calls_refreshed = 0  # entrées périmées re-téléchargées
        self.spent_usd = 0.0
        self.budget: dict | None = None
        self._budget_checked = 0.0

    # ------------------------------------------------------------------ infrastructure
    def _session(self) -> requests.Session:
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=self.pool_size))
            s.headers["User-Agent"] = "tableau-bord-excellence/1.1" + (f" (mailto:{self.mailto})" if self.mailto else "")
            self._local.s = s
        return s

    def _cache_path(self, path: str, params: dict) -> Path:
        from cache_policy import PATH_SCOPE
        key = path + "?" + urlencode(sorted(params.items()))
        h = hashlib.sha1(key.encode()).hexdigest()
        return self.cache_dir / PATH_SCOPE.get(path, "comptages") / h[:2] / f"{h}.json"

    def _fresh(self, cp: Path, path: str) -> bool:
        if not cp.exists():
            return False
        if self.policy is None:
            return True
        from cache_policy import PATH_SCOPE
        return self.policy.is_fresh(cp, PATH_SCOPE.get(path, "comptages"))

    def is_cached(self, path: str, params: dict) -> bool:
        """Vrai si une réponse FRAÎCHE est en cache (une entrée périmée compte comme à refaire)."""
        return self._fresh(self._cache_path(path, {k: v for k, v in params.items() if v is not None}), path)

    def _acquire(self):
        """Attend un jeton (débit global) et une éventuelle pause collective après un 429."""
        while True:
            with self._lock:
                now = time.monotonic()
                if now < self._pause_until:
                    wait = self._pause_until - now
                else:
                    self._tokens = min(max(1.0, self.cur_rps), self._tokens + (now - self._last_refill) * self.cur_rps)
                    self._last_refill = now
                    if self._tokens >= 1:
                        self._tokens -= 1
                        return
                    wait = (1 - self._tokens) / self.cur_rps
            time.sleep(wait)

    def _enter(self):
        with self._cond:
            while self._active >= self.conc_limit:
                self._cond.wait()
            self._active += 1

    def _leave(self):
        with self._cond:
            self._active -= 1
            self._cond.notify_all()

    def _global_pause(self, retry_after: float | None = None):
        """429 : pause collective, débit −30 % et simultanéité divisée par 2 (une fois par salve).
        Les salves rapprochées allongent la pause (2 s, 4 s, 8 s… jusqu'à 60 s)."""
        with self._lock:
            now = time.monotonic()
            self.n_throttled += 1
            new_burst = now - self._last_429 > 2.0
            if new_burst:
                self._backoff = min(60.0, self._backoff * 2) if now - self._last_429 < 30 else 2.0
                self.cur_rps = max(1.0, self.cur_rps * 0.7)
                self._tokens = 0.0
                with self._cond:
                    self.conc_limit = max(2, self.conc_limit // 2)
                    self._ok_streak = 0
            self._last_429 = now
            wait = retry_after if retry_after else self._backoff
            self._pause_until = max(self._pause_until, now + wait + random.random())

    def _ok(self):
        """Succès : remontée progressive du débit et de la simultanéité."""
        with self._lock:
            if self.cur_rps < self.rps:
                self.cur_rps = min(self.rps, self.cur_rps + 0.1)
        with self._cond:
            self._ok_streak += 1
            if self._ok_streak >= 50 and self.conc_limit < self.conc_max:
                self.conc_limit += 1
                self._ok_streak = 0
                self._cond.notify_all()

    def _auth_params(self) -> dict:
        q = {}
        if self.api_key:
            q["api_key"] = self.api_key  # jamais inclus dans la clé de cache
        if self.mailto:
            q["mailto"] = self.mailto
        return q

    # ------------------------------------------------------------------ budget
    def check_budget(self, force: bool = False) -> dict | None:
        """Interroge /rate-limit (gratuit). Renvoie {remaining_usd, cost, calls_left, resets_in}."""
        if not self.api_key:
            return None
        if self.budget and not force and time.monotonic() - self._budget_checked < 30:
            return self.budget
        try:
            r = self._session().get(BASE + "/rate-limit", params=self._auth_params(), timeout=self.timeout)
            r.raise_for_status()
            d = r.json()
            d = d.get("rate_limit", d) if isinstance(d, dict) else {}
        except (requests.RequestException, ValueError):
            return self.budget
        costs = d.get("endpoint_costs_usd") or {}
        cost = DEFAULT_LIST_COST
        if isinstance(costs, dict):
            for k, v in costs.items():
                if "list" in str(k).lower() and isinstance(v, (int, float)) and v > 0:
                    cost = float(v)
                    break
        daily = float(d.get("daily_remaining_usd") or 0)
        prepaid = float(d.get("prepaid_remaining_usd") or 0) if self.allow_prepaid else 0.0
        remaining = daily + prepaid
        self.budget = {"remaining_usd": remaining, "daily_remaining_usd": daily,
                       "prepaid_remaining_usd": float(d.get("prepaid_remaining_usd") or 0),
                       "cost": cost, "calls_left": int(remaining / cost),
                       "resets_in": int(d.get("resets_in_seconds") or 0)}
        self._budget_checked = time.monotonic()
        return self.budget

    def set_reserve(self) -> dict | None:
        """Fixe la réserve : (1 − part) du budget restant au démarrage reste aux autres applications."""
        b = self.check_budget(force=True)
        if b:
            self.reserve_usd = b["remaining_usd"] * (1 - self.budget_share)
        return b

    def _check_reserve(self):
        if self.reserve_usd is None:
            return
        b = self.check_budget(force=True)
        if b is not None and b["remaining_usd"] <= self.reserve_usd:
            raise BudgetExceeded(
                f"Part du budget allouée à cette extraction consommée (réserve de {self.reserve_usd:.2f} $ "
                "laissée à vos autres applications). Relancez plus tard ou augmentez --part-budget ; "
                "le cache conserve l'avancement.")

    def _budget_message(self) -> str:
        b = self.budget or {}
        h = b.get("resets_in", 0) // 3600
        m = (b.get("resets_in", 0) % 3600) // 60
        extra = ("" if self.allow_prepaid or not b.get("prepaid_remaining_usd")
                 else " (solde prépayé disponible : relancez avec --autoriser-prepaye pour l'utiliser)")
        return (f"Budget OpenAlex du jour épuisé — remise à zéro dans {h} h {m:02d}{extra}. "
                "Relancez la même commande ensuite : le cache conserve l'avancement.")

    # ------------------------------------------------------------------ requêtes
    def get(self, path: str, params: dict | None = None, use_cache: bool = True) -> dict:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        cp = self._cache_path(path, params)
        stale = cp.exists() and not self._fresh(cp, path)
        if use_cache and cp.exists() and not stale:
            try:
                data = json.loads(cp.read_text())
                with self._lock:
                    self.calls_cache += 1
                return data
            except ValueError:
                cp.unlink(missing_ok=True)  # fichier tronqué : on refait la requête
        with self._lock:
            if self.max_calls is not None and self.calls_network >= self.max_calls:
                raise BudgetExceeded(f"Plafond de {self.max_calls} requêtes atteint pour cette exécution.")
            self.calls_network += 1  # réservé avant l'appel (évite de dépasser en parallèle)
            periodic = self.calls_network % 250 == 0
        if periodic:
            self._check_reserve()
        q = {**params, **self._auth_params()}
        err = ""
        n_err = n_429 = 0
        while n_err < 6 and n_429 < 8:   # doc OpenAlex : recul exponentiel, ~5 essais
            self._acquire()
            self._enter()
            try:
                r = self._session().get(BASE + path, params=q, timeout=self.timeout)
            except requests.RequestException as e:
                err = e.__class__.__name__
                n_err += 1
                time.sleep(min(30, 2 ** n_err + random.random()))
                continue
            finally:
                self._leave()
            if r.status_code == 200:
                self._ok()
                data = r.json()
                cost = (data.get("meta") or {}).get("cost_usd")
                with self._lock:
                    self.spent_usd += float(cost) if isinstance(cost, (int, float)) else DEFAULT_LIST_COST
                cp.parent.mkdir(parents=True, exist_ok=True)
                tmp = cp.with_suffix(f".{threading.get_ident()}.tmp")
                tmp.write_text(json.dumps(data))
                os.replace(tmp, cp)  # écriture atomique (remplace l'éventuelle version périmée)
                if stale:
                    with self._lock:
                        self.calls_refreshed += 1
                return data
            if r.status_code == 429 and "not_enough_credits" in r.text:
                raise BudgetExceeded(self._budget_message())
            if r.status_code == 429 and _is_too_complex(r.text):
                with self._lock:
                    self.n_split += 1
                raise QueryTooComplex(r.text[:300])
            if r.status_code == 429:
                b = self.check_budget(force=True)
                if b is not None and b["calls_left"] < 1:
                    raise BudgetExceeded(self._budget_message())
                if not self._first_429_logged:
                    self._first_429_logged = True
                    hdr = {k: v for k, v in r.headers.items() if k.lower().startswith(("x-ratelimit", "retry-after"))}
                    print(f"\n  ℹ Premier 429 d'OpenAlex (le script ralentit et réessaie) : {r.text[:400]!r} {hdr}")
                try:
                    ra = float(r.headers.get("Retry-After", ""))
                except ValueError:
                    ra = None
                self._global_pause(ra)
                n_429 += 1
                err = "HTTP 429"
                continue
            if r.status_code in (400, 401, 403, 404):
                raise RuntimeError(f"OpenAlex {r.status_code} sur {path} {params}: {r.text[:300]}")
            err = f"HTTP {r.status_code}"
            n_err += 1
            time.sleep(min(30, 2 ** n_err + random.random()))
        raise RuntimeError(f"OpenAlex inaccessible après plusieurs essais ({err}) : {path} {params}")

    # ------------------------------------------------------------------ helpers
    def iter_cursor(self, path: str, params: dict, per_page: int = 100):
        cursor = "*"
        while cursor:
            data = self.get(path, {**params, "per_page": min(per_page, 100), "cursor": cursor})
            yield from data.get("results", [])
            cursor = data.get("meta", {}).get("next_cursor")
            if not data.get("results"):
                break

    def store_count_by_year(self, filters: list[str], counts: dict[int, int]) -> None:
        """Met en cache un comptage recomposé (somme de sous-requêtes) sous la clé de la requête d'origine."""
        cp = self._cache_path("/works", self.count_by_year_params(filters))
        cp.parent.mkdir(parents=True, exist_ok=True)
        data = {"meta": {"recompose": True},
                "group_by": [{"key": str(y), "count": c} for y, c in sorted(counts.items())]}
        tmp = cp.with_suffix(f".{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, cp)

    @staticmethod
    def count_by_year_params(filters: list[str]) -> dict:
        return {"filter": ",".join(filters), "group_by": "publication_year"}

    def count_by_year(self, filters: list[str]) -> dict[int, int]:
        """Nombre de publications par année (group_by=publication_year)."""
        data = self.get("/works", self.count_by_year_params(filters))
        out: dict[int, int] = {}
        for g in data.get("group_by", []):
            try:
                out[int(g["key"])] = int(g["count"])
            except (ValueError, TypeError):
                continue
        return out
