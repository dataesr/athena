"""
affiliations.py
===============
Rattachement des participations (projets ERC, ANR, prix...) aux établissements : code commun
à toutes les sources, pour que tout compte selon la même règle.

1. rattacher_structures(p)   règle de scanR (scanr-backend, projects.get_participations_simple) :
     - la structure participante (entities_id : Paysage, ROR, RNSR, UAI, SIRET ou SIREN via les
       identifiants de Paysage ; PIC si la source l'indique (open data Horizon : entities_id_source
       vide = PIC, « siren » = SIREN) ; un SIRET absent de Paysage ou sans ROR -> son SIREN ; un SIREN ->
       la structure Paysage de ce SIREN qui a un ROR, la structure de tête s'il y en a plusieurs :
       délégations régionales -> CNRS, centres -> CEA),
       remplacée par son
       successeur Paysage si elle a été fusionnée ou renommée (structure actuelle) ;
     - TOUTES les tutelles de la structure (relation « établissement tutelle » seulement, jamais
       les « établissements participants ») : celles fournies par la source (colonne tutelles :
       pour scanR, les institutions de la structure dans le projet, comme les participations de
       scanR, qui font foi), sinon celles du référentiel scanR (referentiel/rnsr_tutelles.parquet)
       pour la structure et pour chaque laboratoire RNSR (colonne rnsr, valeurs séparées par
       ; , ou |) ; chacune remplacée par son successeur Paysage : le
       projet compte aussi pour les universités et écoles tutelles du laboratoire, pas
       seulement pour l'organisme bénéficiaire (CNRS...).
   Entrée : une ligne par participation, colonnes entities_id, rnsr (et ce que l'on veut garder).
2. tutelles_ror(rors)        chaque ROR et les ROR de ses tutelles OpenAlex (lignée du snapshot,
                             corrigée par LINEAGE_CUTS, moins ETAB_EXCLUDE), comme les articles.
3. lignes_entites(...)       une ligne par (participation, entité) : monde, pays (outre-mer sous
                             FR), établissements (structure, tutelles RNSR, lignée).

Utilisé par erc.py, prix.py, anr.py.
"""

import re
from pathlib import Path

import duckdb
import pandas as pd

from .common import MONDE_ID, pays_entite, pays_list_sql
from .config import DATA_DIR, ETAB_EXCLUDE, LINEAGE_CUTS, REFERENTIEL_DIR
from .referentiel import (charger_correspondance_paysage, charger_siret_paysage, charger_successeurs_paysage,
                          charger_tutelles_rnsr, successeur_final)

# ROR court : 0 + 6 caractères (alphabet Crockford, sans i, l, o, u) + 2 chiffres
ROR_RE = re.compile(r"^0[a-hj-km-np-tv-z0-9]{6}[0-9]{2}$")
# Numéro RNSR : année (4 chiffres) + 5 chiffres + lettre de contrôle, ex. 199719338M
RNSR_RE = re.compile(r"^[0-9]{9}[A-Z]$")


def _rnsrs(v) -> list:
    if not isinstance(v, str):
        return []
    return [x for x in (t.strip().upper() for t in re.split(r"[;|,]", v)) if RNSR_RE.match(x)]


def rattacher_structures(p: pd.DataFrame, ref_dir: str = REFERENTIEL_DIR, successeurs: bool = True,
                         tutelles_rnsr: bool = True) -> tuple:
    """(p, liens) :
    p      : participations + ligne (numéro de ligne), type_identifiant (ror | paysage | rnsr |
             siret | siren | uai | pic | inconnu | vide), ror (ROR actuel de la structure participante, ou None),
             successeur (vrai si la structure a été remplacée par son successeur),
             nb_tutelles_rnsr (tutelles trouvées par le RNSR, avec un ROR),
             ids_participants (structure telle quelle + tutelles RNSR après successeur Paysage :
             même construction que les participations de scanR, voir controle_scanr.py)
    liens  : (ligne, ror, origine) : un lien par établissement auquel la participation
             est rattachée ; origine = structure | tutelle_rnsr"""
    p = p.reset_index(drop=True).copy()
    p["ligne"] = range(len(p))
    corresp = charger_correspondance_paysage(ref_dir)
    p2r = dict(zip(corresp["referentiel_id"], corresp["ror"]))
    r2p_tous = corresp.groupby("ror")["referentiel_id"].agg(list)
    r2p = {r: v[0] for r, v in r2p_tous.items() if len(v) == 1}   # ROR -> Paysage, si sans ambiguïté
    succ = charger_successeurs_paysage(ref_dir) if successeurs else {}
    tut = charger_tutelles_rnsr(ref_dir) if tutelles_rnsr else pd.DataFrame(columns=["rnsr", "tutelle_id"])
    tutelles = tut.groupby("rnsr")["tutelle_id"].agg(list).to_dict()
    ids = charger_siret_paysage(ref_dir)
    uai2p = dict(zip(ids.loc[ids["type"] == "uai", "valeur"], ids.loc[ids["type"] == "uai", "paysage_id"]))
    pic2p = (ids[ids["type"] == "pic"].drop_duplicates("valeur").set_index("valeur")["paysage_id"].to_dict())
    sir = ids[ids["type"] == "siret"]
    siret2p = dict(zip(sir["valeur"], sir["paysage_id"]))

    def ror_actuel(pid):
        """ROR de la structure Paysage actuelle (successeur), à défaut celui de pid."""
        fin = successeur_final(pid, succ)
        return p2r.get(fin) or p2r.get(pid), fin != pid

    # SIREN -> la structure Paysage de ce SIREN qui a un ROR ; s'il y en a plusieurs (délégations
    # régionales, centres...), la seule structure de tête ; sinon ambigu, non rattaché
    sir_ror = sir.assign(siren=sir["valeur"].str[:9],
                         ror=[ror_actuel(x)[0] for x in sir["paysage_id"]]).dropna(subset=["ror"])
    siren2p = {}
    for siren, g in sir_ror.groupby("siren"):
        for cand in (g, g[g["tete"].astype(bool)]):
            if cand["ror"].nunique() == 1:
                siren2p[siren] = cand["paysage_id"].iloc[0]
                break

    def par_siret(v):
        """Paysage d'un SIRET (14 chiffres) ou SIREN (9) : SIRET exact s'il mène à un ROR, sinon SIREN."""
        pid = siret2p.get(v) if len(v) == 14 else None
        if pid and ror_actuel(pid)[0]:
            return pid
        return siren2p.get(v[:9])

    rors, types, remplacees, liens, nb_tut, ids_part = [], [], [], [], [], []
    # Tutelles fournies par la source (colonne facultative « tutelles », liste d'identifiants
    # Paysage) : pour scanR, les institutions « établissement tutelle » de la structure dans le
    # projet, exactement comme les participations de scanR. Sinon (None) : référentiel scanR.
    tut_source = p["tutelles"] if "tutelles" in p.columns else pd.Series([None] * len(p))
    # Type d'identifiant donné par la source (open data Horizon : entities_id_source) : un nombre
    # à 9 chiffres est un SIREN si la source le dit (« siren »), un PIC si elle ne dit rien ou
    # « pic » (open data Horizon : entities_id = generalpic) ; sans colonne : SIREN, sinon PIC.
    src = (p["entities_id_source"].fillna("").astype(str).str.strip().str.lower()
           if "entities_id_source" in p.columns else pd.Series([None] * len(p)))
    for ligne, i, rn, ts, so in zip(p["ligne"], p["entities_id"].fillna(""), p["rnsr"], tut_source, src):
        norm = i.strip().rstrip("/").split("/")[-1]
        ror, remplace, labos = None, False, _rnsrs(rn)
        if not i:
            t = "vide"
        elif ROR_RE.match(norm.lower()):
            t, ror = "ror", norm.lower()
            if ror in r2p:
                autre, remplace = ror_actuel(r2p[ror])
                ror = autre if remplace and autre else ror
                remplace = remplace and bool(autre)
        elif i in p2r or i in succ:
            t = "paysage"
            ror, remplace = ror_actuel(i)
        elif RNSR_RE.match(i.strip().upper()):
            t = "rnsr"
            labos.append(i.strip().upper())
        elif norm.isdigit() and len(norm) == 9 and so in ("", "pic") and norm in pic2p:
            t = "pic"
            ror, remplace = ror_actuel(pic2p[norm])
        elif norm.isdigit() and len(norm) in (9, 14) and so != "pic" and par_siret(norm):
            t = "siret" if len(norm) == 14 else "siren"
            ror, remplace = ror_actuel(par_siret(norm))
        elif norm.isdigit() and len(norm) == 9 and so is None and norm in pic2p:
            t = "pic"
            ror, remplace = ror_actuel(pic2p[norm])
        elif norm.upper() in uai2p:
            t = "uai"
            ror, remplace = ror_actuel(uai2p[norm.upper()])
        else:
            t = "inconnu"
        rors.append(ror)
        types.append(t)
        remplacees.append(bool(remplace and ror))
        if ror:
            liens.append((ligne, ror, "structure"))
        n = 0
        # identifiants des participants, au format des participations de scanR (contrôle :
        # controle_scanr.py) : la structure telle quelle + ses tutelles RNSR (successeur Paysage)
        # la structure (telle quelle, même non reconnue : scanR garde par ex. le PIC) et les
        # laboratoires RNSR sont des participants dans scanR
        # identifiant de la structure comme dans scanR : la fiche Paysage quand la source donne un
        # PIC ou un ROR que Paysage connaît (sans ambiguïté), sinon l'identifiant tel quel
        id_struct = i.strip()
        if t == "pic":
            id_struct = pic2p[norm]
        elif t == "ror" and norm.lower() in r2p:
            id_struct = r2p[norm.lower()]
        ids = ([id_struct] if id_struct else []) + [x for x in dict.fromkeys(labos)
                                                    if x.upper() != i.strip().upper()]
        if isinstance(ts, (list, tuple)):          # tutelles de la source (scanR)
            liste_tut = [x for x in ts if x]
        else:                                      # référentiel : structure elle-même + ses RNSR
            liste_tut = [tid for cle in dict.fromkeys([i.strip()] + labos) for tid in tutelles.get(cle, [])]
        for tid in dict.fromkeys(liste_tut):
            ids.append(successeur_final(tid, succ))
            r, _ = ror_actuel(tid)
            if r:
                liens.append((ligne, r, "tutelle_rnsr"))
                n += 1
        nb_tut.append(n)
        ids_part.append(sorted(set(ids)))
    p["type_identifiant"], p["ror"], p["successeur"], p["nb_tutelles_rnsr"] = types, rors, remplacees, nb_tut
    p["ids_participants"] = ids_part
    liens = pd.DataFrame(liens, columns=["ligne", "ror", "origine"]).drop_duplicates(["ligne", "ror"])
    return p, liens


def tutelles_ror(rors, data_dir: str = DATA_DIR, lineage_cuts=LINEAGE_CUTS,
                 etab_exclude=ETAB_EXCLUDE) -> pd.DataFrame:
    """(ror, entite_id) : chaque ROR et les ROR de ses tutelles (lignée OpenAlex
    corrigée par LINEAGE_CUTS), moins ETAB_EXCLUDE. Sans lignée disponible : le ROR seul."""
    data_dir = Path(data_dir)
    base = pd.DataFrame({"ror": sorted(set(rors))})
    base["entite_id"] = base["ror"]
    if not ((data_dir / "inst_lineage.parquet").exists() and (data_dir / "institutions.parquet").exists()):
        print(f"Lignée OpenAlex absente de {data_dir} : pas de rattachement aux tutelles")
        return base
    from .lineage_check import apply_lineage_cuts
    excl = pays_list_sql([r.split("/")[-1] for r in (etab_exclude or [])]) or "NULL"
    with duckdb.connect() as con:
        apply_lineage_cuts(con, data_dir, lineage_cuts, table="lin_ok")
        con.register("base", base[["ror"]])
        anc = con.execute(f"""
            SELECT DISTINCT i.ror, l.anc_ror AS entite_id
            FROM lin_ok l
            JOIN read_parquet('{data_dir}/institutions.parquet') i ON i.inst_id = l.inst_id
            WHERE i.ror IN (SELECT ror FROM base)
              AND l.anc_ror IS NOT NULL AND l.anc_ror <> '' AND l.anc_ror NOT IN ({excl})
        """).df()
    return pd.concat([base, anc], ignore_index=True).drop_duplicates()


def lignes_entites(d: pd.DataFrame, liens: pd.DataFrame, lien: pd.DataFrame, cols: list,
                   monde: bool = True) -> pd.DataFrame:
    """Une ligne par (participation, entité) : cols + entite_type, entite_id.
    d     : participations (cols, ligne, pays_code) ;
    liens : (ligne, ror) de rattacher_structures ;
    lien  : (ror, entite_id) de tutelles_ror (ou identité).
    Pays : pays de la participation, outre-mer regroupé sous FR (PAYS_FR, même règle partout)."""
    parts = []
    if monde:
        parts.append(d[cols].assign(entite_type="monde", entite_id=MONDE_ID))
    parts.append(d.dropna(subset=["pays_code"])
                 .assign(entite_type="pays",
                         entite_id=lambda x: pays_entite(x["pays_code"]))
                 [cols + ["entite_type", "entite_id"]])
    li = liens.merge(d[["ligne"] + [c for c in cols if c not in liens.columns]], on="ligne")
    parts.append(li[cols + ["ror"]].merge(lien, on="ror")
                 .assign(entite_type="etablissement")[cols + ["entite_type", "entite_id"]])
    return pd.concat(parts, ignore_index=True).drop_duplicates()
