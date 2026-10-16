"""
openalex_s3.py
==============
Accès aux fichiers du snapshot OpenAlex, quelle que soit la source :

  "public" : bucket public s3://openalex (snapshot trimestriel, sans clé),
             lu en HTTPS ; seul le dernier snapshot est disponible.
  "daily"  : bucket de l'offre payante s3://openalex-snapshots/full/<date>/parquet/,
             un snapshot complet par jour. Accès avec la clé OPENALEX_API_KEY
             de config.py (lue dans l'environnement), échangée contre des identifiants
             S3 temporaires auprès de https://api.openalex.org/snapshots/credentials
             (renouvelés automatiquement avant expiration).

Toutes les fonctions renvoient des « manifests » au même format :
    {"date": ..., "source": ..., "files": [{"url": "s3://...", "meta": {...}}]}
pour que l'extraction ne dépende pas de la source.

Exemples :
    from project.server.main.openalex_s3 import list_daily_snapshots, snapshot_manifest
    list_daily_snapshots()                        # dates disponibles (offre payante)
    snapshot_manifest("works", source="daily")    # dernier snapshot quotidien
    snapshot_manifest("works", source="daily", date="2026-10-14")
"""

import datetime as dt
import json
import threading
import urllib.error
import urllib.request
from urllib.parse import urlparse

import duckdb

from .config import (DAILY_BUCKET, DAILY_PREFIX, OPENALEX_API_KEY, S3_REGION,
                     SNAPSHOT_SOURCE)

PUBLIC_BUCKET = "openalex"
PUBLIC_HTTP = "https://openalex.s3.amazonaws.com"
CREDENTIALS_URL = "https://api.openalex.org/snapshots/credentials"

_lock = threading.Lock()
_creds = {}


# --- Identifiants (offre payante) ---------------------------------------------------

_ALIASES = {
    "AccessKeyId": ("AccessKeyId", "accessKeyId", "access_key_id", "aws_access_key_id"),
    "SecretAccessKey": ("SecretAccessKey", "secretAccessKey", "secret_access_key",
                        "aws_secret_access_key"),
    "SessionToken": ("SessionToken", "sessionToken", "session_token", "aws_session_token",
                     "Token", "token"),
    "Expiration": ("Expiration", "expiration", "expires_at", "expiresAt"),
    "Endpoint": ("Endpoint", "endpoint", "endpoint_url", "EndpointUrl"),
    "Region": ("Region", "region", "region_name"),
    "Bucket": ("Bucket", "bucket"),
}


def _find_creds(obj):
    """Cherche le dict qui contient la clé d'accès, à n'importe quel niveau
    (réponse « credential_process » à plat, ou imbriquée type {"Credentials": {...}})."""
    if isinstance(obj, dict):
        if any(k in obj for k in _ALIASES["AccessKeyId"]):
            return obj
        for v in obj.values():
            found = _find_creds(v)
            if found is not None:
                return found
    return None


def _normalize(raw: dict) -> dict:
    inner = _find_creds(raw)
    if inner is None:
        raise RuntimeError("réponse d'OpenAlex sans clé d'accès S3 ; clés reçues : "
                           f"{sorted(raw) if isinstance(raw, dict) else type(raw).__name__}")
    out = {}
    for std, names in _ALIASES.items():
        for src in (inner, raw):            # les infos annexes peuvent être au 1er niveau
            v = next((src[n] for n in names if n in src and src[n]), None)
            if v is not None:
                out[std] = v
                break
    return out


def _fetch_credentials() -> tuple:
    """Appelle OpenAlex ; renvoie (réponse brute, identifiants normalisés)."""
    key = OPENALEX_API_KEY
    if not key:
        raise RuntimeError("clé API absente : définir la variable d'environnement "
                           "OPENALEX_API_KEY (nécessaire pour la source 'daily')")
    req = urllib.request.Request(f"{CREDENTIALS_URL}?api_key={key}", method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            raw = json.load(r)
    except urllib.error.HTTPError as e:   # message sans l'URL, qui contient la clé
        raise RuntimeError(f"OpenAlex a refusé la demande d'identifiants S3 "
                           f"(HTTP {e.code}) : vérifier la clé et l'abonnement") from None
    return raw, _normalize(raw)


def daily_credentials(min_validity_s: int = 600) -> dict:
    """Identifiants S3 temporaires pour le bucket de l'offre payante.
    Mis en cache et renouvelés s'ils expirent dans moins de min_validity_s."""
    with _lock:
        exp = _creds.get("_expiration")
        now = dt.datetime.now(dt.timezone.utc)
        if exp and (exp - now).total_seconds() > min_validity_s:
            return dict(_creds)
        _, c = _fetch_credentials()
        exp_s = c.get("Expiration")
        _creds.clear()
        _creds.update(c)
        _creds["_expiration"] = (dt.datetime.fromisoformat(str(exp_s).replace("Z", "+00:00"))
                                 if exp_s else now + dt.timedelta(minutes=30))
        return dict(_creds)


def _s3_client():
    import boto3
    c = daily_credentials()
    return boto3.client("s3", region_name=c.get("Region") or S3_REGION,
                        endpoint_url=c.get("Endpoint"),
                        aws_access_key_id=c["AccessKeyId"],
                        aws_secret_access_key=c["SecretAccessKey"],
                        aws_session_token=c.get("SessionToken"))


def _mask(v) -> str:
    v = str(v)
    return f"{v[:4]}…({len(v)} car.)" if len(v) > 8 else "***"


def diagnose_daily() -> dict:
    """Diagnostic de l'accès aux snapshots quotidiens, SANS afficher de secret :
      1. forme de la réponse d'OpenAlex (clés, préfixe de la clé d'accès,
         présence du jeton de session, expiration, endpoint/région éventuels) ;
      2. validité des identifiants auprès d'AWS (STS GetCallerIdentity) ;
      3. liste du bucket.
    Préfixe de clé : ASIA = identifiants temporaires (exigent le jeton de
    session), AKIA = clé permanente."""
    import boto3
    out = {}
    raw, c = _fetch_credentials()
    def forme(o, depth=0):
        if isinstance(o, dict) and depth < 3:
            return {k: forme(v, depth + 1) for k, v in o.items()}
        return type(o).__name__
    out["reponse_openalex"] = forme(raw)
    out["AccessKeyId"] = _mask(c.get("AccessKeyId", ""))
    out["SessionToken_present"] = bool(c.get("SessionToken"))
    out["Expiration"] = c.get("Expiration")
    out["Endpoint"] = c.get("Endpoint")
    out["Region"] = c.get("Region") or f"(aucune, défaut config : {S3_REGION})"
    kw = dict(aws_access_key_id=c["AccessKeyId"], aws_secret_access_key=c["SecretAccessKey"],
              aws_session_token=c.get("SessionToken"))
    try:
        ident = boto3.client("sts", region_name="us-east-1", **kw).get_caller_identity()
        out["sts"] = f"OK (compte {_mask(ident.get('Account'))})"
    except Exception as e:
        out["sts"] = f"ÉCHEC : {type(e).__name__}: {str(e)[:150]}"
    try:
        s3 = boto3.client("s3", region_name=c.get("Region") or S3_REGION,
                          endpoint_url=c.get("Endpoint"), **kw)
        r = s3.list_objects_v2(Bucket=DAILY_BUCKET, Prefix=f"{DAILY_PREFIX}/", Delimiter="/",
                               MaxKeys=5)
        out["s3_list"] = "OK : " + ", ".join(p["Prefix"] for p in r.get("CommonPrefixes", []))
    except Exception as e:
        out["s3_list"] = f"ÉCHEC : {type(e).__name__}: {str(e)[:150]}"
    for k, v in out.items():
        print(f"{k:22s} {v}")
    return out


# --- Connexion DuckDB --------------------------------------------------------------

def _sql_str(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def connect(install: bool = False, daily: bool = False) -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB avec httpfs ; identifiants S3 si daily=True."""
    con = duckdb.connect()
    if install:
        con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    if daily:
        c = daily_credentials()
        con.execute(f"""
            CREATE OR REPLACE SECRET openalex_daily (
                TYPE s3,
                KEY_ID {_sql_str(c['AccessKeyId'])},
                SECRET {_sql_str(c['SecretAccessKey'])},
                SESSION_TOKEN {_sql_str(c.get('SessionToken', ''))},
                REGION {_sql_str(c.get('Region') or S3_REGION)},
                {f"ENDPOINT {_sql_str(urlparse(c['Endpoint']).netloc or c['Endpoint'])}," if c.get('Endpoint') else ""}
                SCOPE {_sql_str(f's3://{DAILY_BUCKET}')}
            )
        """)
    return con


def is_daily(url: str) -> bool:
    return url.startswith(f"s3://{DAILY_BUCKET}/")


def readable_url(url: str) -> str:
    """URL lisible par DuckDB : HTTPS pour le bucket public, s3:// sinon."""
    if url.startswith(f"s3://{PUBLIC_BUCKET}/"):
        return PUBLIC_HTTP + url[len(f"s3://{PUBLIC_BUCKET}"):]
    return url


def connect_for(url: str, install: bool = False) -> duckdb.DuckDBPyConnection:
    return connect(install=install, daily=is_daily(url))


# --- Liste des snapshots et des fichiers ---------------------------------------------

def list_daily_snapshots() -> list:
    """Dates des snapshots quotidiens disponibles (offre payante), de la plus ancienne
    à la plus récente."""
    s3 = _s3_client()
    dates, token = [], None
    while True:
        kw = dict(Bucket=DAILY_BUCKET, Prefix=f"{DAILY_PREFIX}/", Delimiter="/")
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        dates += [p["Prefix"].rstrip("/").split("/")[-1] for p in r.get("CommonPrefixes", [])]
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    return sorted(dates)


def _daily_manifest(entity: str, date: str) -> dict:
    """Liste des fichiers Parquet d'une entité dans un snapshot quotidien.
    meta = taille + ETag (identifient un fichier inchangé d'un snapshot à l'autre)."""
    s3 = _s3_client()
    prefix = f"{DAILY_PREFIX}/{date}/parquet/{entity}/"
    files, token = [], None
    while True:
        kw = dict(Bucket=DAILY_BUCKET, Prefix=prefix)
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                files.append({"url": f"s3://{DAILY_BUCKET}/{o['Key']}",
                              "meta": {"content_length": o["Size"],
                                       "etag": o.get("ETag", "").strip('"')}})
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    if not files:
        raise FileNotFoundError(f"aucun fichier Parquet sous s3://{DAILY_BUCKET}/{prefix}")
    return {"date": date, "source": "daily", "entity": entity,
            "files": sorted(files, key=lambda f: f["url"])}


def _public_manifest(entity: str) -> dict:
    url = f"{PUBLIC_HTTP}/data/parquet/{entity}/manifest.json"
    with urllib.request.urlopen(url) as r:
        m = json.load(r)
    m["source"] = "public"
    return m


def snapshot_manifest(entity: str = "works", source: str = SNAPSHOT_SOURCE,
                      date: str = None) -> dict:
    """Manifest d'une entité (works, sources, institutions...).

    source "public" : snapshot public actuel (date ignorée).
    source "daily"  : snapshot quotidien `date` (défaut : le plus récent).
    """
    if source == "public":
        if date:
            m = _public_manifest(entity)
            if str(m.get("date")) != str(date):
                raise ValueError(f"le bucket public ne contient que le snapshot {m.get('date')} ; "
                                 f"pour le {date}, utiliser source='daily'")
            return m
        return _public_manifest(entity)
    if source == "daily":
        return _daily_manifest(entity, date or list_daily_snapshots()[-1])
    raise ValueError(f"source inconnue : {source!r} (attendu : 'public' ou 'daily')")
