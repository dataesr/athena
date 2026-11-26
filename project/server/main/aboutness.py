"""
aboutness.py
============
Topic OpenAlex d'un texte libre (titre + résumé de projet, de candidature...) par le endpoint
« aboutness » d'OpenAlex (/text/topics), le même classifieur que celui qui attribue les topics
aux works et aux awards (version ABOUTNESS_VERSION). Code commun : ANR, piliers européens...

Limites d'OpenAlex (https://help.openalex.org/api/tag-aboutness, vérifiées le 2026-11-17) :
  - 1 requête par seconde (ABOUTNESS_INTERVALLE_S) : ~86 400 textes par jour au plus ;
  - 0,01 $ par requête (100 crédits), décompté du budget de la clé (OPENALEX_API_KEY) ;
  - 20 à 2000 caractères, titre et résumé ensemble : le résumé est tronqué, un texte trop
    court n'est pas envoyé (pas de dépense) ;
  - la tarification d'OpenAlex marque /text comme « deprecated » : à surveiller.

Garde-fous :
  - cache (ABOUTNESS_DIR) : un texte déjà classé n'est jamais renvoyé (clé = empreinte de la
    version + du texte envoyé) ; écrit au fil de l'eau, une interruption ne perd rien ; garde
    la réponse JSON complète (keywords, autres topics...), pas seulement le topic principal ;
  - budget par lancement (ABOUTNESS_BUDGET_USD) ; arrêt aussi quand le budget restant du jour,
    lu dans l'en-tête X-RateLimit-Remaining-USD, passe sous ABOUTNESS_MARGE_USD ;
  - 429 : attente (Retry-After) puis nouvel essai ; budget épuisé (401/402/403) : arrêt propre ;
  - erreur passagère (5xx, réseau, délai dépassé) : nouveaux essais espacés (ABOUTNESS_ATTENTES_S),
    puis le texte est sauté (non mis en cache, repris au lancement suivant) et on continue ;
  - texte refusé (autre 4xx, ex. 400) : mis en cache comme « rejeté : HTTP 400 : <message> »
    (pas renvoyé à chaque lancement), mais seulement après un succès qui suit, preuve que l'API
    fonctionne (sinon une panne côté OpenAlex ferait rejeter des textes valides) ;
  - ABOUTNESS_ECHECS_MAX textes de suite en échec : arrêt (OpenAlex en panne).
  Ce qui n'a pas été classé (budget, panne, texte sauté) le sera au lancement suivant ;
  attrs["complet"] dit s'il reste des textes à reprendre.

Exemple :
    from project.server.main.aboutness import estimer, classer_textes
    t = pd.DataFrame({"cle": [...], "titre": [...], "resume": [...]})
    estimer(t)                      # nombre à envoyer, coût, durée (sans appel)
    r = classer_textes(t)           # cle, topic_id, topic_score, aboutness_note
    lire_cache(complet=True)        # + réponse JSON complète (keywords, autres topics...)
"""

import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import duckdb
import pandas as pd

from .config import (ABOUTNESS_ATTENTES_S, ABOUTNESS_BUDGET_USD, ABOUTNESS_DIR, ABOUTNESS_ECHECS_MAX,
                     ABOUTNESS_INTERVALLE_S, ABOUTNESS_MARGE_USD, ABOUTNESS_URL, ABOUTNESS_VERSION,
                     OPENALEX_API_KEY)

COUT_USD = 0.01
# colonnes du cache et leur type ; « reponse » = réponse JSON complète d'OpenAlex (topics,
# keywords, concepts, meta...), avec le texte envoyé et la date d'appel. Les lots écrits avant
# 2026-11-19 n'ont que les quatre premières colonnes (les autres lues à NULL).
_COLONNES = {"empreinte": "VARCHAR", "topic_id": "VARCHAR", "topic_score": "DOUBLE",
             "aboutness_note": "VARCHAR", "titre_envoye": "VARCHAR", "resume_envoye": "VARCHAR",
             "reponse": "VARCHAR", "date_appel": "VARCHAR"}
_TYPES = ", ".join(f"{c}::{t} AS {c}" for c, t in _COLONNES.items())
MIN_CAR, MAX_CAR = 20, 2000


def texte_envoye(titre, resume) -> tuple:
    """(titre, résumé) tels qu'envoyés : nettoyés, résumé tronqué pour tenir en MAX_CAR
    caractères au total ; (None, None) si trop court."""
    t = " ".join(str(titre or "").split())[:MAX_CAR]
    r = " ".join(str(resume or "").split())
    r = r[:max(0, MAX_CAR - len(t) - 1)]
    if len(t) + len(r) < MIN_CAR:
        return None, None
    return t, r


def empreinte(titre: str, resume: str, version: int = ABOUTNESS_VERSION) -> str:
    return hashlib.sha1(f"v{version}\x1f{titre}\x1f{resume}".encode("utf-8")).hexdigest()


def _cache_dir(version: int) -> Path:
    return Path(ABOUTNESS_DIR) / f"topics_v{version}"


def lire_cache(version: int = ABOUTNESS_VERSION, complet: bool = False) -> pd.DataFrame:
    """Textes déjà classés : empreinte, topic_id, topic_score, aboutness_note ; complet=True
    ajoute titre_envoye, resume_envoye, reponse (JSON complet d'OpenAlex) et date_appel."""
    cols = list(_COLONNES) if complet else list(_COLONNES)[:4]
    d = _cache_dir(version)
    if not d.exists() or not any(d.glob("*.parquet")):
        return pd.DataFrame(columns=cols)
    src = f"read_parquet('{d}/*.parquet', union_by_name = true)"
    presentes = set(duckdb.sql(f"SELECT * FROM {src} LIMIT 0").columns)
    sel = ", ".join(f"{c if c in presentes else 'NULL'}::{_COLONNES[c]} AS {c}" for c in cols)
    return duckdb.sql(f"SELECT {sel} FROM {src}").df().drop_duplicates("empreinte")


def _preparer(textes: pd.DataFrame, version: int) -> pd.DataFrame:
    t = textes[["cle", "titre", "resume"]].copy()
    env = [texte_envoye(a, b) for a, b in zip(t["titre"], t["resume"])]
    t["titre_envoye"] = [e[0] for e in env]
    t["resume_envoye"] = [e[1] for e in env]
    t["empreinte"] = [empreinte(a, b, version) if a is not None else None for a, b in env]
    return t


def estimer(textes: pd.DataFrame, version: int = ABOUTNESS_VERSION) -> dict:
    """Sans appel : textes à envoyer (hors cache et hors trop courts), coût et durée."""
    t = _preparer(textes, version)
    cache = set(lire_cache(version)["empreinte"])
    a_envoyer = t["empreinte"].dropna()
    a_envoyer = a_envoyer[~a_envoyer.isin(cache)].nunique()
    return {"textes": len(t), "trop_courts": int(t["empreinte"].isna().sum()),
            "deja_classes": int(t["empreinte"].isin(cache).sum()), "a_envoyer": int(a_envoyer),
            "cout_usd": round(a_envoyer * COUT_USD, 2),
            "duree_heures": round(a_envoyer * ABOUTNESS_INTERVALLE_S / 3600, 1)}


def _appel(titre: str, resume: str, version: int) -> tuple:
    """(réponse JSON, budget restant du jour en $ ou None, statut) ; statut 0 = erreur réseau ou
    délai dépassé ; en cas d'erreur, la réponse porte « retry_after » et « message » (début du
    corps de la réponse d'erreur, jamais l'URL qui contient la clé)."""
    q = {"api_key": OPENALEX_API_KEY} if OPENALEX_API_KEY else {}
    if version != 2:
        q["version"] = str(version)
    url = ABOUTNESS_URL + ("?" + urllib.parse.urlencode(q) if q else "")
    corps = json.dumps({"title": titre, **({"abstract": resume} if resume else {})}).encode("utf-8")
    req = urllib.request.Request(url, data=corps, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            reste = r.headers.get("X-RateLimit-Remaining-USD")
            return json.load(r), (float(reste) if reste else None), 200
    except urllib.error.HTTPError as e:     # message sans l'URL, qui contient la clé
        try:
            msg = " ".join(e.read(500).decode("utf-8", "replace").split())[:200]
        except Exception:
            msg = ""
        return {"retry_after": e.headers.get("Retry-After"), "message": msg}, None, e.code
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        return {"retry_after": None, "message": type(e).__name__}, None, 0


def _passagere(statut: int) -> bool:
    return statut == 429 or statut == 0 or statut >= 500


def classer_textes(textes: pd.DataFrame, version: int = ABOUTNESS_VERSION,
                   budget_usd: float = ABOUTNESS_BUDGET_USD, marge_usd: float = ABOUTNESS_MARGE_USD,
                   intervalle_s: float = ABOUTNESS_INTERVALLE_S, lot: int = 200,
                   verbose: bool = True) -> pd.DataFrame:
    """Topic de chaque texte (colonnes cle, titre, resume ; cle = identifiant de l'appelant).
    Renvoie cle, topic_id ('T…', None si non classé), topic_score, aboutness_note
    (None si classé ; « trop court », « non classé : budget », « sans topic : <note OpenAlex> »...)."""
    t = _preparer(textes, version)
    cache_dir = _cache_dir(version)
    cache_dir.mkdir(parents=True, exist_ok=True)
    connus = set(lire_cache(version)["empreinte"])
    a_faire = t.dropna(subset=["empreinte"]).drop_duplicates("empreinte")
    a_faire = a_faire[~a_faire["empreinte"].isin(connus)]
    if verbose:
        e = estimer(textes, version)
        print(f"Aboutness v{version} : {e['textes']:,} textes, {e['deja_classes']:,} en cache, "
              f"{e['trop_courts']:,} trop courts, {e['a_envoyer']:,} à envoyer "
              f"(~{e['cout_usd']} $, ~{e['duree_heures']} h) ; budget du lancement : {budget_usd} $")

    nouveaux, depense, arret = [], 0.0, None
    rejets_en_attente = []          # 4xx : écrits seulement après un succès (l'API fonctionne)
    sautes = {}                     # empreinte -> motif (erreur passagère persistante)
    echecs_de_suite, reste = 0, None

    def ecrire():
        if nouveaux:
            f = cache_dir / f"lot_{time.strftime('%Y%m%d_%H%M%S')}_{len(list(cache_dir.glob('*.parquet')))}.parquet"
            duckdb.from_df(pd.DataFrame(nouveaux)).project(_TYPES).write_parquet(str(f))
            nouveaux.clear()

    def ligne(r, rep, tid=None, score=None, note=None):
        return {"empreinte": r.empreinte, "topic_id": tid, "topic_score": score, "aboutness_note": note,
                "titre_envoye": r.titre_envoye, "resume_envoye": r.resume_envoye,
                "reponse": json.dumps(rep, ensure_ascii=False),
                "date_appel": time.strftime("%Y-%m-%dT%H:%M:%S")}

    t0 = time.time()
    for k, r in enumerate(a_faire.itertuples(), 1):
        if depense + COUT_USD > budget_usd + 1e-9:
            arret = f"budget du lancement atteint ({budget_usd} $)"
            break
        for essai in range(len(ABOUTNESS_ATTENTES_S) + 1):
            debut = time.time()
            rep, reste_appel, statut = _appel(r.titre_envoye, r.resume_envoye, version)
            time.sleep(max(0.0, intervalle_s - (time.time() - debut)))
            if not _passagere(statut) or essai == len(ABOUTNESS_ATTENTES_S):
                break
            attente = ABOUTNESS_ATTENTES_S[essai]
            if statut == 429 and rep.get("retry_after"):
                try:
                    attente = max(attente, float(rep["retry_after"]))
                except ValueError:
                    pass
            if verbose and essai >= 2:
                print(f"  OpenAlex HTTP {statut or 'réseau'} ({rep.get('message')}) : "
                      f"essai {essai + 2} dans {attente:.0f} s")
            time.sleep(attente)
        if reste_appel is not None:
            reste = reste_appel
        if statut in (401, 402, 403):
            arret = f"refus OpenAlex (HTTP {statut}) : clé ou budget du jour épuisé"
            break
        if statut == 200:
            echecs_de_suite = 0
            depense += COUT_USD
            nouveaux.extend(rejets_en_attente)      # l'API répond : les rejets sont bien dus au texte
            rejets_en_attente.clear()
            pt = rep.get("primary_topic") or {}
            tid = (pt.get("id") or "").rsplit("/", 1)[-1] or None
            nouveaux.append(ligne(r, rep, tid, pt.get("score"),
                                  None if tid else f"sans topic : {(rep.get('meta') or {}).get('note')}"))
        else:
            echecs_de_suite += 1
            motif = f"HTTP {statut or 'réseau'}" + (f" : {rep['message']}" if rep.get("message") else "")
            if _passagere(statut):
                sautes[r.empreinte] = f"erreur OpenAlex ({motif})"
            else:
                rejets_en_attente.append(ligne(r, rep, note=f"rejeté : {motif}"))
            if verbose:
                print(f"  texte {r.cle} sauté : {motif}")
            if echecs_de_suite >= ABOUTNESS_ECHECS_MAX:
                arret = f"{echecs_de_suite} textes de suite en erreur OpenAlex (dernière : {motif})"
                break
        if len(nouveaux) >= lot:
            ecrire()
        if verbose and k % 500 == 0:
            print(f"  {k:,}/{len(a_faire):,} textes, {depense:.2f} $, "
                  f"{(time.time() - t0) / 60:.0f} min" + (f", reste du jour {reste:.0f} $" if reste is not None else ""))
        if reste is not None and reste < marge_usd:
            arret = f"budget restant du jour sous la marge ({reste:.0f} $ < {marge_usd} $)"
            break
    ecrire()            # les rejets encore en attente (non confirmés par un succès) sont oubliés
    for x in rejets_en_attente:
        sautes[x["empreinte"]] = f"erreur OpenAlex ({x['aboutness_note'].removeprefix('rejeté : ')})"
    if verbose:
        print(f"Aboutness : {depense:.2f} $ dépensés en {(time.time() - t0) / 60:.0f} min"
              + (f" ; {len(sautes):,} textes sautés (erreur OpenAlex)" if sautes else "")
              + (f" ; ARRÊT : {arret}" if arret else "")
              + (" (relancer pour la suite)" if arret or sautes else ""))

    cache = lire_cache(version)
    out = t[["cle", "empreinte"]].merge(cache, on="empreinte", how="left")
    out["aboutness_note"] = out["aboutness_note"].astype(object)
    out.loc[t["empreinte"].isna().values, "aboutness_note"] = "trop court"
    manque = out["empreinte"].notna() & out["topic_score"].isna() & out["aboutness_note"].isna()
    if manque.any():
        motifs = out.loc[manque, "empreinte"].map(sautes).astype(object).fillna(arret or "inconnu")
        out.loc[manque, "aboutness_note"] = "non classé : " + motifs
    out.attrs.update(depense_usd=round(depense, 2), arret=arret, textes_sautes=len(sautes),
                     complet=not bool(manque.any()))
    return out[["cle", "topic_id", "topic_score", "aboutness_note"]]
