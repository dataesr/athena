"""
controle_scanr.py
=================
Contrôle de cohérence des participants calculés ici (affiliations.rattacher_structures) avec
les participations publiées par scanR (participations_denormalized.jsonl.gz), qui FONT FOI pour
les projets scanR (ANR...).

Construction de scanR (scanr-backend, projects.get_participations_simple, vérifiée sur le
commit 21e04a4 du 13/11/2026) : pour chaque participation d'un projet dont la structure est
identifiée (participants[].structure), une ligne pour la structure d'accueil (structure.id,
participation_structure_accueil = True) et une ligne par institution de cette structure dont
relationType == « établissement tutelle » (les « établissements participants » sont exclus),
remplacée par son successeur Paysage (participation_structure_accueil = False).
Ici : ids_participants de rattacher_structures (structure telle quelle + tutelles RNSR du
référentiel scanR, après successeur Paysage).

Comparaison participation par participation (participation_id) des ensembles d'identifiants :
  manquants   dans scanR, absents ici   (ex. tutelle manquante dans le référentiel)
  en_trop     ici, absents de scanR     (ex. « établissement participant » compté à tort)
Alerte (logger.warning + champ alerte du résultat) si la part de participations divergentes
dépasse SCANR_SEUIL_ALERTE ; arrêt du calcul si SCANR_ALERTE_BLOQUANTE.

Exemple :
    from project.server.main.controle_scanr import controler_participants
    res = controler_participants(parts, types=("ANR",))   # parts : participation_id, ids_participants
"""

import datetime as dt
import gzip
import json
import urllib.request
from pathlib import Path

import pandas as pd

from .config import (PARTICIPATIONS_SCANR_URL, PROJETS_DIR, PROJETS_SCANR_URL, SCANR_ALERTE_BLOQUANTE,
                     SCANR_SEUIL_ALERTE)
from .logger import get_logger
from .projets_scanr import download_scanr

logger = get_logger(__name__)


def participants_scanr(projets_dir: str = PROJETS_DIR, types=None, refresh: bool = False,
                       verbose: bool = True, cle: str = "participation_id") -> pd.DataFrame:
    """Participants de scanR : une ligne par participation (cle = "participation_id") ou par projet
    (cle = "project_id"), ids_scanr (ensemble des participant_id), id_accueil (structure d'accueil,
    au niveau participation seulement)."""
    f = download_scanr(projets_dir, PARTICIPATIONS_SCANR_URL, refresh=refresh, verbose=verbose)
    par, accueil = {}, {}
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for line in fh:
            d = json.loads(line)
            if types and d.get("project_type") not in types:
                continue
            pid, part = d.get(cle), d.get("participant_id")
            if not pid or not part:
                continue
            par.setdefault(pid, set()).add(str(part))
            if d.get("participation_structure_accueil"):
                accueil[pid] = str(part)
    return pd.DataFrame({cle: list(par), "ids_scanr": [sorted(v) for v in par.values()],
                         "id_accueil_scanr": [accueil.get(k) for k in par]})


def _date_publication(url: str):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30) as r:
            v = r.headers.get("Last-Modified")
        return dt.datetime.strptime(v, "%a, %d %b %Y %H:%M:%S %Z").date() if v else None
    except Exception:          # contrôle informatif seulement
        return None


def controler_participants(parts: pd.DataFrame, types=None, projets_dir: str = PROJETS_DIR,
                           out_csv: str = None, seuil: float = SCANR_SEUIL_ALERTE,
                           bloquant: bool = SCANR_ALERTE_BLOQUANTE, verbose: bool = True,
                           cle: str = "participation_id", seulement_communs: bool = False,
                           libelle: str = "participations") -> dict:
    """Compare parts (cle, ids_participants) aux participations de scanR, par participation
    (cle = "participation_id" : ANR, numérotation identique) ou par projet (cle = "project_id" :
    Horizon, numérotation des participations différente entre l'open data et scanR).
    seulement_communs : ne compare que les clés présentes des deux côtés (les autres sont comptées).
    Renvoie un résumé (valeurs simples) ; écrit le détail des écarts dans out_csv."""
    ref = participants_scanr(projets_dir, types, verbose=verbose, cle=cle)
    ici = (parts[[cle, "ids_participants"]]
           .assign(ids_participants=lambda x: x["ids_participants"].map(lambda v: sorted(set(v or []))))
           .groupby(cle)["ids_participants"]
           .agg(lambda s: sorted(set().union(*map(set, s)))).reset_index())
    m = ici.merge(ref, on=cle, how="outer", indicator=True)
    nb_ici_seul, nb_scanr_seul = int((m["_merge"] == "left_only").sum()), int((m["_merge"] == "right_only").sum())
    if seulement_communs:
        m = m[m["_merge"] == "both"]
    m["ids_participants"] = m["ids_participants"].map(lambda v: v if isinstance(v, list) else [])
    m["ids_scanr"] = m["ids_scanr"].map(lambda v: v if isinstance(v, list) else [])
    m["manquants"] = [sorted(set(b) - set(a)) for a, b in zip(m["ids_participants"], m["ids_scanr"])]
    m["en_trop"] = [sorted(set(a) - set(b)) for a, b in zip(m["ids_participants"], m["ids_scanr"])]
    # participations sans participant ni ici ni dans scanR (partenaires non identifiés) : hors contrôle
    m = m[(m["ids_participants"].map(len) > 0) | (m["ids_scanr"].map(len) > 0)]
    ecart = m[(m["manquants"].map(len) > 0) | (m["en_trop"].map(len) > 0)]
    n, ne = len(m), len(ecart)
    part_ecart = ne / n if n else 0.0

    d_proj, d_part = _date_publication(PROJETS_SCANR_URL), _date_publication(PARTICIPATIONS_SCANR_URL)
    res = {
        "fichier_reference": PARTICIPATIONS_SCANR_URL,
        "dates_publication": {"projets": str(d_proj), "participations": str(d_part)},
        "niveau": cle,
        "nb_participations": n,
        "nb_participations_ecart": ne,
        "pct_participations_ecart": round(100 * part_ecart, 2),
        "nb_participations_absentes_scanr": nb_ici_seul,
        "nb_participations_absentes_ici": nb_scanr_seul,
        "nb_identifiants_manquants": int(m["manquants"].map(len).sum()),
        "nb_identifiants_en_trop": int(m["en_trop"].map(len).sum()),
        "seuil_alerte_pct": round(100 * seuil, 2),
    }
    alertes = []
    if part_ecart > seuil:
        alertes.append(f"{ne:,} {libelle} sur {n:,} ({100 * part_ecart:.2f} %) ont des participants "
                       f"différents de scanR (seuil {100 * seuil:.2f} %)")
    if d_proj and d_part and abs((d_proj - d_part).days) > 1:
        alertes.append(f"fichiers scanR publiés à des dates différentes : projets {d_proj}, "
                       f"participations {d_part}")
    res["alerte"] = " ; ".join(alertes) or None

    if out_csv:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
        (ecart.assign(manquants=ecart["manquants"].map("|".join), en_trop=ecart["en_trop"].map("|".join),
                      ids_participants=ecart["ids_participants"].map("|".join),
                      ids_scanr=ecart["ids_scanr"].map("|".join),
                      presence=ecart["_merge"].map({"both": "les deux", "left_only": "ici seulement",
                                                    "right_only": "scanR seulement"}))
         [[cle, "presence", "manquants", "en_trop", "ids_participants", "ids_scanr"]]
         .to_csv(out_csv, sep=";", index=False, encoding="utf-8"))
        res["fichier_ecarts"] = str(out_csv)

    if verbose:
        print(f"Contrôle scanR ({libelle}) : {n:,} comparé(e)s, {ne:,} avec écart "
              f"({res['pct_participations_ecart']} %) ; identifiants manquants {res['nb_identifiants_manquants']:,}, "
              f"en trop {res['nb_identifiants_en_trop']:,}")
    if res["alerte"]:
        logger.warning(f"ALERTE cohérence scanR : {res['alerte']}" + (f" (détail : {out_csv})" if out_csv else ""))
        if bloquant:
            raise RuntimeError(f"incohérence avec les participations de scanR : {res['alerte']}")
    return res


def controler_horizon_scanr(laureats: pd.DataFrame, ref_dir: str = None, projets_dir: str = PROJETS_DIR,
                            out_csv: str = None, verbose: bool = True) -> dict:
    """Lauréats Horizon Europe (participations de l'open data, horizon.participations, TOUS les
    programmes) : participants calculés ici (règle commune, affiliations.rattacher_structures)
    comparés, projet par projet, aux participations Horizon Europe de scanR, qui font foi. Seuls
    les projets présents des deux côtés sont comparés."""
    from .affiliations import rattacher_structures
    from .config import REFERENTIEL_DIR
    p = laureats[laureats["project_id"].notna()].copy()
    p, _ = rattacher_structures(p, ref_dir or REFERENTIEL_DIR)
    return controler_participants(p, types=("Horizon Europe",), projets_dir=projets_dir, out_csv=out_csv,
                                  verbose=verbose, cle="project_id", seulement_communs=True,
                                  libelle="projets Horizon Europe lauréats")


# --- Montants : les participations de scanR font foi -------------------------------------------

def montants_scanr(projets_dir: str = PROJETS_DIR, types=None, verbose: bool = True) -> tuple:
    """(participations, projets) des montants de scanR (participations_denormalized) :
      participations : participation_id, montant (participation_funding)
      projets        : project_id, budget_total (project_budgetTotal), budget_finance
                       (project_budgetFinanced)
    Valeurs identiques sur toutes les lignes d'une participation (structure d'accueil et
    tutelles) : la première non vide est gardée."""
    f = download_scanr(projets_dir, PARTICIPATIONS_SCANR_URL, verbose=verbose)
    part, proj = {}, {}
    num = lambda v: float(v) if isinstance(v, (int, float)) and v == v else None
    with gzip.open(f, "rt", encoding="utf-8") as fh:
        for line in fh:
            d = json.loads(line)
            if types and d.get("project_type") not in types:
                continue
            pa, pr = d.get("participation_id"), d.get("project_id")
            if pa and part.get(pa) is None:
                part[pa] = num(d.get("participation_funding"))
            if pr:
                bt, bf = proj.get(pr, (None, None))
                proj[pr] = (bt if bt is not None else num(d.get("project_budgetTotal")),
                            bf if bf is not None else num(d.get("project_budgetFinanced")))
    return (pd.DataFrame({"participation_id": list(part), "montant": list(part.values())}),
            pd.DataFrame({"project_id": list(proj), "budget_total": [v[0] for v in proj.values()],
                          "budget_finance": [v[1] for v in proj.values()]}))


def _remplacer(df: pd.DataFrame, ref: pd.DataFrame, cle_df: str, cle_ref: str, cols: list) -> tuple:
    """Valeurs de ref (scanR participations) à la place de celles de df quand ref en a une ;
    sinon valeur de df (base des projets). Renvoie (df, nb de conflits, nb remplis par ref)."""
    m = df.merge(ref.rename(columns={cle_ref: cle_df, **{c: f"{c}__ref" for c in cols}}), on=cle_df, how="left")
    conflits, completes = 0, 0
    src = pd.Series("scanr_projets", index=m.index)
    for c in cols:
        a, b = pd.to_numeric(m[c], errors="coerce"), m[f"{c}__ref"]
        conflits += int(((a - b).abs() > 1e-6).sum())
        completes += int((a.isna() & b.notna()).sum())
        src = src.where(b.isna(), "scanr_participations")
        m[c] = b.where(b.notna(), a)
    m["source_montant"] = src.where(m[[f"{c}__ref" for c in cols]].notna().any(axis=1) |
                                    m[cols].notna().any(axis=1), None)
    return m.drop(columns=[f"{c}__ref" for c in cols]), conflits, completes


def appliquer_montants_scanr(projets: pd.DataFrame, parts: pd.DataFrame = None, types=None,
                             projets_dir: str = PROJETS_DIR, cle_projet: str = "projet_id",
                             bloquant: bool = SCANR_ALERTE_BLOQUANTE, verbose: bool = True) -> tuple:
    """Montants des participations de scanR, qui font foi : projets (cle_projet, budget_total,
    budget_finance) et parts (participation_id, montant) prennent les valeurs de
    participations_denormalized ; à défaut (partenaires ou projets sans participation identifiée
    dans scanR), celles de la base des projets. source_montant = scanr_participations |
    scanr_projets. Alerte si les deux fichiers de scanR donnent des montants différents.
    Renvoie (projets, parts, controle)."""
    ref_parts, ref_proj = montants_scanr(projets_dir, types, verbose=verbose)
    projets, c_proj, comp_proj = _remplacer(projets, ref_proj, cle_projet, "project_id",
                                            ["budget_total", "budget_finance"])
    c_part, comp_part = 0, 0
    if parts is not None:
        parts, c_part, comp_part = _remplacer(parts, ref_parts, "participation_id", "participation_id", ["montant"])
    controle = {
        "source": PARTICIPATIONS_SCANR_URL,
        "nb_projets_montant_participations": int((projets["source_montant"] == "scanr_participations").sum()),
        "nb_projets_montant_base_projets": int((projets["source_montant"] == "scanr_projets").sum()),
        "nb_budgets_conflits": c_proj, "nb_budgets_completes": comp_proj,
    }
    if parts is not None:
        controle.update({
            "nb_participations_montant_participations": int((parts["source_montant"] == "scanr_participations").sum()),
            "nb_participations_montant_base_projets": int((parts["source_montant"] == "scanr_projets").sum()),
            "nb_montants_participation_conflits": c_part, "nb_montants_participation_completes": comp_part,
        })
    n_conf = c_proj + c_part
    controle["alerte"] = (f"{n_conf:,} montants différents entre les participations et la base des projets "
                          f"de scanR (les participations font foi)") if n_conf else None
    if verbose:
        print(f"Montants scanR : {controle['nb_projets_montant_participations']:,} projets et "
              f"{controle.get('nb_participations_montant_participations', 0):,} participations depuis les "
              f"participations ; conflits {n_conf}")
    if controle["alerte"]:
        logger.warning(f"ALERTE montants scanR : {controle['alerte']}")
        if bloquant:
            raise RuntimeError(controle["alerte"])
    return projets, parts, controle
