#!/usr/bin/env python3
"""Assemble le tableau de bord en UN fichier HTML autonome, données chiffrées.

- Le DSFR (CSS, JS, polices Marianne, icônes) est intégré : aucun appel externe.
- Les données (data/biblio.json) sont compressées puis chiffrées en AES-256-GCM.
  La clé de données est elle-même chiffrée pour chaque utilisateur avec une clé
  dérivée de son mot de passe (PBKDF2-SHA256, 600 000 itérations).
  Le fichier HTML ne contient AUCUN mot de passe, ni en clair ni haché de façon
  exploitable : sans mot de passe valide, les données sont illisibles.

Usage :
  python pipeline/build_dashboard.py --data data/biblio.json --user dgri --user cabinet
      → demande les mots de passe au clavier (non affichés)
  python pipeline/build_dashboard.py --data data/biblio.json --users-file comptes.csv
      → comptes.csv : login;mot_de_passe  (fichier à NE PAS versionner)
  DASH_USERS="dgri:motdepasse1;cabinet:motdepasse2" python pipeline/build_dashboard.py ...
"""
from __future__ import annotations

import argparse
import base64
import csv
import getpass
import gzip
import hashlib
import json
import os
import re
import sys
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
VENDOR = WEB / "vendor" / "dsfr"
b64 = lambda b: base64.b64encode(b).decode()  # noqa: E731


def uid(gsalt: bytes, login: str) -> str:
    return hashlib.sha256(gsalt + b":" + login.strip().lower().encode()).hexdigest()


def encrypt(data: bytes, users: list[tuple[str, str]], iterations: int) -> dict:
    dek = AESGCM.generate_key(bit_length=256)
    iv = os.urandom(12)
    ct = AESGCM(dek).encrypt(iv, gzip.compress(data, 9), None)
    gsalt = os.urandom(16)
    entries = []
    for login, pwd in users:
        if len(pwd) < 12:
            print(f"  ⚠ mot de passe de « {login} » court (< 12 caractères) : préférez une phrase de passe.")
        salt, wiv = os.urandom(16), os.urandom(12)
        kek = PBKDF2HMAC(hashes.SHA256(), 32, salt, iterations).derive(pwd.encode())
        entries.append({"uid": uid(gsalt, login), "salt": b64(salt), "iv": b64(wiv),
                        "wk": b64(AESGCM(kek).encrypt(wiv, dek, None))})
    return {"v": 1, "iter": iterations, "gsalt": b64(gsalt), "users": entries, "iv": b64(iv), "ct": b64(ct)}


def data_uri(path: Path) -> str:
    mime = {".woff2": "font/woff2", ".svg": "image/svg+xml", ".png": "image/png"}[path.suffix]
    return f"data:{mime};base64,{b64(path.read_bytes())}"


def inline_urls(css: str, base: Path) -> str:
    def rep(m):
        u = m.group(1).strip("'\"")
        if u.startswith("data:") or u.startswith("#"):
            return m.group(0)
        p = (base / u).resolve()
        return f'url("{data_uri(p)}")' if p.exists() else m.group(0)
    return re.sub(r"url\(([^)]+)\)", rep, css)


def dsfr_css(used_icons: set[str]) -> str:
    css = (VENDOR / "dsfr.min.css").read_text()
    css = re.sub(r',url\([^)]*\.woff\) format\("woff"\)', "", css)       # woff2 seul
    css = re.sub(r"@font-face\{[^}]*Spectral[^}]*\}", "", css)           # police non utilisée
    css = inline_urls(css, VENDOR)
    icons = (VENDOR / "utility" / "icons" / "icons.min.css").read_text()
    keep = []
    for rule in icons.split("}"):
        sel = rule.split("{")[0]
        if any(re.search(rf"\.{re.escape(i)}(?![\w-])", sel) for i in used_icons):
            keep.append(rule + "}")
    return css + "\n" + inline_urls("".join(keep), VENDOR / "utility" / "icons")


def read_users(args) -> list[tuple[str, str]]:
    users = []
    if args.users_file:
        for row in csv.reader(Path(args.users_file).open(encoding="utf-8"), delimiter=";"):
            if len(row) >= 2 and row[0].strip() and not row[0].startswith("#"):
                users.append((row[0].strip(), row[1]))
    env = os.environ.get("DASH_USERS")
    if env:
        for part in env.split(";"):
            if ":" in part:
                lg, pw = part.split(":", 1)
                users.append((lg.strip(), pw))
    for lg in args.user or []:
        pw = getpass.getpass(f"Mot de passe pour « {lg} » : ")
        if pw != getpass.getpass("Confirmer : "):
            sys.exit("Les mots de passe ne correspondent pas.")
        users.append((lg, pw))
    if not users:
        sys.exit("Aucun compte : utilisez --user, --users-file ou DASH_USERS.")
    return users


def add_topics(data: dict, data_path: Path) -> bool:
    """Ajoute le rattachement topics → axes aux données produites avant cette fonctionnalité.

    Reconstruit sans aucune requête, à partir du cache des topics OpenAlex
    (data/cache/topics.json) et de config/axes.json : ces fichiers doivent être ceux de l'extraction.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import referentiel
    cache = data_path.parent / "cache" / "topics.json"
    if not cache.exists():
        print(f"  ℹ {cache} introuvable : la liste des topics par discipline ne sera pas affichée "
              "(relancez extract_openalex.py --dry-run pour la produire).")
        return False
    cfg = ROOT / "config"
    topics = json.loads(cache.read_text())
    params = json.loads((cfg / "params.json").read_text())
    discs, _ = referentiel.build(json.loads((cfg / "axes.json").read_text()), topics,
                                 params.get("seuil_sous_domaine_pour_axes_par_mots_cles", 0.1))
    data["topics"] = referentiel.dashboard_topics(discs, topics)
    n = sum(len(v) for v in data["topics"]["by_axis"].values())
    print(f"  ℹ topics par discipline reconstruits depuis {cache.name} et config/axes.json ({n} rattachements).")
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=str(ROOT / "data" / "biblio.json"))
    ap.add_argument("--out", default=str(ROOT / "dist" / "tableau-de-bord.html"))
    ap.add_argument("--user", action="append", help="identifiant (mot de passe demandé au clavier)")
    ap.add_argument("--users-file", help="CSV login;mot_de_passe")
    ap.add_argument("--iterations", type=int, default=600_000)
    args = ap.parse_args()

    users = read_users(args)
    raw = Path(args.data).read_bytes()
    data = json.loads(raw)
    if not data.get("topics") and not (data.get("meta") or {}).get("demo"):
        tp = add_topics(data, Path(args.data))
        if tp:
            raw = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    payload = encrypt(raw, users, args.iterations)

    tpl = (WEB / "index.html").read_text()
    app_css = (WEB / "app.css").read_text()
    app_js = (WEB / "app.js").read_text()
    used_icons = set(re.findall(r"fr-icon-[a-z0-9-]+", tpl + app_js))
    html = (tpl
            .replace("/*__DSFR_CSS__*/", dsfr_css(used_icons))
            .replace("/*__APP_CSS__*/", app_css)
            .replace("/*__DSFR_JS__*/", (VENDOR / "dsfr.module.min.js").read_text())
            .replace("/*__APP_JS__*/", app_js)
            .replace("__PAYLOAD__", json.dumps(payload)))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(html, encoding="utf-8")
    print(f"écrit {args.out} ({Path(args.out).stat().st_size/1e6:.1f} Mo) — comptes : {', '.join(u for u, _ in users)}")


if __name__ == "__main__":
    main()
