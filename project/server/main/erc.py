"""
erc.py
======
Bourses ERC : candidatures et lauréats par établissement, pays et panel, pour
l'onglet ERC du dashboard. Mêmes clés que la bibliométrie (CONTRAT_DONNEES.md) :
entite_id (ROR court, code ISO du pays, MONDE) et discipline_code (panel ERC :
LS1…LS9, PE1…PE11, SH1…SH8), pour partager les filtres (établissement français,
discipline, comparaison internationale).

Source
------
  Lignes ERC (thema_code = ERC_THEMA) des deux jeux Horizon Europe de l'open data ESR,
  lus par horizon.py (une ligne par participation) :
    soumis    propositions évaluées (stage = evaluated), structure au dépôt
    laureats  projets financés (stage = successful), structure à la signature
  Seules les lignes des porteurs comptent (erc_role = ERC_ROLE_PI) : une par projet pour
  STG, COG, ADG et POC, 2 à 4 pour les Synergy (SYG, un PI par ligne).

Règles
------
  Couverture : appels Horizon Europe (2021 et après). Les appels Horizon 2020 ne sont
  pas dans ces jeux.
  Deux modes de comptage, au même grain :
    projets  nombre de project_id distincts (nb_candidatures, nb_laureats...)
    PI       nombre de porteurs (lignes PI distinctes : nb_pi_candidats, nb_pi_laureats) ;
             ne diffère du premier que pour les Synergy
  Taux de succès = projets lauréats (jeu laureats) / projets évalués (jeu soumis), pour la
  même entité. La candidature est comptée dans la structure de dépôt, le lauréat dans la
  structure de signature (règle des statistiques officielles) :
    nb_candidatures           projets évalués, déposés dans l'entité
    nb_candidatures_retenues  dont financés (project_id présent chez les lauréats), où
                              que le projet soit signé
    nb_laureats               projets financés signés dans l'entité
    nb_laureats_arrivees      signés dans l'entité, non déposés dans l'entité
    nb_laureats_departs       déposés dans l'entité et financés, signés ailleurs
  nb_laureats = nb_candidatures_retenues + nb_laureats_arrivees - nb_laureats_departs
  (vérifié par compute_erc). Pour MONDE, les « arrivées » sont les lauréats absents du
  jeu des soumis (écart entre les deux jeux, 0 attendu).
  Discipline : discipline_code = panel_code, null quand la source n'a pas de panel :
  toujours pour les Synergy et les Proof of Concept, et pour quelques STG / COG / ADG.
  Ces lignes ne comptent que pour « toutes disciplines ». Année, panel et type d'un
  projet : valeur la plus fréquente de ses lignes (lauréats d'abord).
  Établissement (affiliations.rattacher_structures, même logique que scanR) : le projet compte
  pour la structure participante (entities_id : Paysage, ROR ou RNSR) et, pour une
  participation française, pour toutes les tutelles des laboratoires RNSR indiqués
  (numero_national_de_structure, plusieurs valeurs séparées par ;) : un ERC hébergé par le
  CNRS dans une UMR compte aussi pour les universités tutelles de l'UMR. Chaque structure
  est remplacée par son successeur Paysage (fusions, changements de nom), pour tout
  afficher sur les établissements actuels, puis convertie en ROR (table Paysage -> ROR).
  Si ERC_RATTACHEMENT_LIGNEE, le projet compte aussi pour les tutelles OpenAlex de ces
  ROR (même règle que les articles). Les participations sans aucun établissement sont
  listées dans erc_non_rattaches.csv.
  Pays : pays de la structure, en code ISO2 comme OpenAlex (GB et GR), outre-mer sous FR.
  Comptage entier : un projet compte 1 pour chaque entité présente (une Synergy à
  plusieurs établissements compte pour chacun) ; MONDE compte chaque projet une fois.

Sorties (dans ERC_DIR = <DATA_DIR>/erc/)
----------------------------------------
  erc_effectifs.parquet     une ligne par entite_type x entite_id x annee_appel x
                            discipline_code x type_bourse :
                              nb_candidatures, nb_candidatures_retenues, nb_laureats,
                              nb_laureats_arrivees, nb_laureats_departs,
                              nb_pi_candidats, nb_pi_laureats
  erc_entites.parquet       pays et établissements présents : entite_id, entite_type,
                            nom, pays_code, nb_candidatures, nb_candidatures_retenues,
                            nb_laureats
                            (sert à compléter la collection « entites », entites.py)
  erc_meta.json             paramètres, années (dont provisoires), types de bourse,
                            pays comparés, contrôles, textes de méthode
  <HORIZON_DIR>/horizon_controle_scanr.csv  projets Horizon Europe lauréats (tous programmes)
                            dont les participants (structures, laboratoires RNSR, tutelles)
                            diffèrent des participations de scanR, qui font foi (alerte au-delà
                            de SCANR_SEUIL_ALERTE ; erc_meta.controles.coherence_scanr_horizon)
  erc_non_rattaches.csv     structures de dépôt ou de signature sans ROR (à corriger
                            dans Paysage)
  Les copies brutes des jeux sont dans HORIZON_DIR (horizon.py).

Exemple :
    from project.server.main.erc import compute_erc
    res = compute_erc()
    res = compute_erc(telecharger=False)   # réutilise les copies de HORIZON_DIR
"""

import datetime as dt
import json
from pathlib import Path

import duckdb
import pandas as pd

from .common import MONDE_ID, pays_entite, plus_frequent
from .config import (NOMENCLATURE_DISCIPLINAIRE, DATA_DIR, ERC_ANNEES_PROVISOIRES, ERC_DIR, ERC_PAYS_COMPARAISON,
                     ERC_PAYS_COMPARAISON_N, ERC_RATTACHEMENT_LIGNEE, ERC_ROLE_PI,
                     ERC_SEUIL_ANNEE_COMPLETE, ERC_THEMA, ERC_TYPES_BOURSE, ERC_TYPES_PRINCIPAUX,
                     HORIZON_DIR, HORIZON_ODS_LAUREATS, HORIZON_ODS_SOUMIS, PAYS_FR, REFERENTIEL_DIR)
from .affiliations import lignes_entites, rattacher_structures, tutelles_ror
from .horizon import lire_horizon, participations
from .programmes import effectifs_projets, nommer_entites, non_rattaches as non_rattaches_programme
from .disciplines import disciplines_path

CLES = ["annee_appel", "discipline_code", "type_bourse"]
EFFECTIFS = ["nb_candidatures", "nb_candidatures_retenues", "nb_laureats", "nb_laureats_arrivees",
             "nb_laureats_departs", "nb_pi_candidats", "nb_pi_laureats"]

# Libellés des types de bourse (destination_code, en majuscules). Un type absent reçoit son code.
LIBELLES_TYPES_BOURSE = {
    "STG": "Starting Grant",
    "COG": "Consolidator Grant",
    "ADG": "Advanced Grant",
    "SYG": "Synergy Grant",
    "POC": "Proof of Concept",
    "PERA": "Prix d'engagement public pour la recherche (Public Engagement with Research Award)",
    "SJI": "Initiative pour le journalisme scientifique (Science Journalism Initiative)",
    "ERC-OTHER": "Autres actions ERC",
}

def erc_path(nom: str, erc_dir: str = ERC_DIR) -> Path:
    return Path(erc_dir) / nom


def pays_compares_erc(erc_dir: str = ERC_DIR, pays_comparaison=ERC_PAYS_COMPARAISON) -> list:
    """Pays de la comparaison ERC : erc_meta.json si compute_erc() a été lancé, sinon la
    liste imposée ERC_PAYS_COMPARAISON (ou [] si elle est vide). Sert à l'étape 6, qui
    calcule aussi les effectifs bibliométriques de ces pays."""
    f = erc_path("erc_meta.json", erc_dir)
    if f.exists():
        return list(json.loads(f.read_text(encoding="utf-8")).get("pays_compares") or [])
    return list(pays_comparaison or [])


def annees_provisoires(eff: pd.DataFrame, types_principaux=ERC_TYPES_PRINCIPAUX,
                       seuil: float = ERC_SEUIL_ANNEE_COMPLETE, imposees=ERC_ANNEES_PROVISOIRES) -> tuple:
    """(années provisoires, motif par année). Liste imposée si `imposees` n'est pas None ;
    sinon règle automatique (voir config.ERC_ANNEES_PROVISOIRES), sur les lignes MONDE."""
    if imposees is not None:
        return sorted(int(a) for a in imposees), {int(a): "liste imposée (config)" for a in imposees}
    m = eff[(eff["entite_id"] == MONDE_ID) & eff["type_bourse"].isin(types_principaux)]
    par = m.groupby(["annee_appel", "type_bourse"])[["nb_candidatures", "nb_laureats"]].sum()
    tot = m.groupby("annee_appel")["nb_candidatures"].sum()
    motifs = {}
    for a in tot.index:
        autres = tot.drop(a)
        r = []
        for t in types_principaux:
            ailleurs = t in set(par.drop(a, level=0, errors="ignore").index.get_level_values(1))
            c, l = par.loc[(a, t)] if (a, t) in par.index else (0, 0)
            if ailleurs and c == 0:
                r.append(f"aucune candidature {t}")
            elif ailleurs and l == 0:
                r.append(f"aucun lauréat {t}")
        if len(autres) and tot[a] < seuil * autres.median():
            r.append(f"{int(tot[a])} candidatures, moins de {seuil:.0%} de la médiane des autres "
                     f"années ({int(autres.median())})")
        if r:
            motifs[int(a)] = " ; ".join(r)
    return sorted(motifs), motifs


def types_bourse_meta(eff: pd.DataFrame, projets: pd.DataFrame,
                      types_principaux=ERC_TYPES_PRINCIPAUX) -> list:
    """[{code, libelle, avec_panel, principal, nb_candidatures, nb_candidatures_retenues,
    nb_laureats, nb_pi_candidats, nb_pi_laureats, nb_candidatures_sans_panel}] (MONDE) ;
    avec_panel : au moins la moitié des projets du type ont un panel (vrai pour STG, COG,
    ADG ; faux pour SYG, POC)."""
    m = eff[eff["entite_id"] == MONDE_ID].groupby("type_bourse")[EFFECTIFS].sum()
    res = []
    for t, g in projets.groupby("type_bourse"):
        sans_panel = g["discipline_code"].isna()
        e = m.loc[t] if t in m.index else pd.Series(0, index=EFFECTIFS)
        res.append({"code": t, "libelle": LIBELLES_TYPES_BOURSE.get(t, t),
                    "avec_panel": bool(sans_panel.sum() <= len(g) / 2), "principal": t in types_principaux,
                    **{c: int(e[c]) for c in ["nb_candidatures", "nb_candidatures_retenues", "nb_laureats",
                                              "nb_pi_candidats", "nb_pi_laureats"]},
                    "nb_candidatures_sans_panel": int((sans_panel & g["soumis"]).sum())})
    ordre = {t: k for k, t in enumerate(LIBELLES_TYPES_BOURSE)}
    return sorted(res, key=lambda x: (ordre.get(x["code"], len(ordre)), x["code"]))


# --- Préparation ---------------------------------------------------------------------

def lignes_pi(p: pd.DataFrame, thema: str = ERC_THEMA, role_pi: str = ERC_ROLE_PI) -> pd.DataFrame:
    """Lignes ERC des porteurs, à partir de horizon.participations()."""
    erc = (p["thema_code"] == thema.upper()) | (p["action_id"] == thema.upper())
    return p[erc & (p["erc_role"].str.upper() == role_pi.upper())].copy()


def preparer_projets(soumis: pd.DataFrame, laureats: pd.DataFrame, types_bourse=ERC_TYPES_BOURSE) -> tuple:
    """(projets, soumis, laureats) : projets = une ligne par project_id : annee_appel,
    discipline_code, type_bourse (valeur la plus fréquente, lauréats d'abord), soumis,
    laureat ; soumis et laureats restreints aux types retenus."""
    def _attr(d):
        g = d.groupby("project_id")
        return pd.DataFrame({"annee_appel": g["annee_appel"].agg(plus_frequent),
                             "discipline_code": g["panel_code"].agg(plus_frequent),
                             "type_bourse": g["destination_code"].agg(plus_frequent)})
    a_l, a_s = _attr(laureats), _attr(soumis)
    projets = a_l.combine_first(a_s)   # valeurs des lauréats, complétées par celles des soumis
    projets.index.name = "project_id"
    projets = projets.reset_index()
    projets["annee_appel"] = pd.to_numeric(projets["annee_appel"]).astype("Int64")
    projets["soumis"] = projets["project_id"].isin(set(soumis["project_id"]))
    projets["laureat"] = projets["project_id"].isin(set(laureats["project_id"]))
    if types_bourse:
        projets = projets[projets["type_bourse"].isin([t.upper() for t in types_bourse])]
        garde = set(projets["project_id"])
        soumis, laureats = soumis[soumis["project_id"].isin(garde)], laureats[laureats["project_id"].isin(garde)]
    return projets.reset_index(drop=True), soumis, laureats




# --- Calcul ----------------------------------------------------------------------------

def _methode(meta: dict) -> list:
    a = meta["annees"]
    prov = meta["annees_provisoires"]
    sans_panel = ", ".join(t["code"] for t in meta["types_bourse"] if not t["avec_panel"])
    principaux = ", ".join(meta["types_bourse_defaut"])
    return [
        "Source : open data de l'enseignement supérieur et de la recherche, participations aux "
        f"projets Horizon Europe : propositions évaluées (« {meta['source']['soumis']} ») et projets "
        f"financés (« {meta['source']['laureats']} »), lignes ERC des porteurs (PI).",
        (f"Couverture : appels {a[0]} à {a[-1]} (Horizon Europe). " if a else "Couverture : aucune année. ")
        + "Les appels Horizon 2020 (2014-2020) ne sont pas inclus.",
        (f"Années provisoires : {', '.join(map(str, prov))} ; tous les résultats de ces appels ne "
         "sont pas encore publiés, les candidatures et le taux de succès vont évoluer."
         if prov else "Aucune année provisoire : les résultats de tous les appels couverts sont publiés."),
        "Candidature : un projet évalué, compté dans la structure de dépôt. Lauréat : un projet "
        "financé, compté dans la structure de signature (règle des statistiques officielles). "
        "Taux de succès : lauréats / candidatures, pour la même entité, la même période et les mêmes "
        "panels ; un porteur qui change d'établissement entre le dépôt et la signature compte une "
        "candidature dans le premier et un lauréat dans le second, si bien que le taux d'une petite "
        "entité peut dépasser 100 %.",
        "Comptage : en projets (par défaut) ou en porteurs (PI) ; les deux ne diffèrent que pour les "
        "Synergy Grants, qui ont de 2 à 4 porteurs.",
        f"Types de bourse : calcul par défaut sur {principaux}. "
        "Discipline : panel ERC du projet (mêmes codes que les disciplines de la bibliométrie). "
        + (f"Les types {sans_panel} n'ont pas de panel, de même que quelques autres projets : "
           if sans_panel else "Les projets sans panel ")
        + "ils ne comptent que pour « toutes disciplines ».",
        "Établissement : structure de dépôt ou de signature et, pour les participations françaises, "
        "toutes les tutelles des laboratoires (RNSR) où le projet est mené, comme dans scanR : un "
        "projet hébergé par un organisme dans une unité mixte compte aussi pour les universités et "
        "écoles tutelles de l'unité. Les structures fusionnées ou renommées sont remplacées par "
        "l'établissement actuel (successeurs Paysage)"
        + (" ; un projet compte aussi pour les tutelles de ces établissements, comme les articles"
           if meta["parametres"]["rattachement_lignee"] else "") + ".",
        "Pays : pays de la structure de dépôt (candidatures) ou de signature (lauréats), pas la "
        "nationalité du porteur.",
        "Comptage entier : un projet compte pour chaque établissement et chaque pays "
        "(Synergy). Les effectifs sont additionnés avant le calcul des taux et des parts.",
    ]


def compute_erc(erc_dir: str = ERC_DIR, horizon_dir: str = HORIZON_DIR, data_dir: str = DATA_DIR,
                ref_dir: str = REFERENTIEL_DIR, nomenclature: str = NOMENCLATURE_DISCIPLINAIRE,
                thema: str = ERC_THEMA, role_pi: str = ERC_ROLE_PI, types_bourse=ERC_TYPES_BOURSE,
                rattachement_lignee: bool = ERC_RATTACHEMENT_LIGNEE,
                pays_comparaison=ERC_PAYS_COMPARAISON, pays_comparaison_n: int = ERC_PAYS_COMPARAISON_N,
                types_principaux=ERC_TYPES_PRINCIPAUX, annees_provisoires_imposees=ERC_ANNEES_PROVISOIRES,
                seuil_annee_complete: float = ERC_SEUIL_ANNEE_COMPLETE,
                telecharger: bool = True, verbose: bool = True) -> dict:
    """Calcule les effectifs ERC et écrit les sorties dans erc_dir (voir l'en-tête).
    telecharger=False : relit les copies de horizon_dir. Renvoie un résumé."""
    out = Path(erc_dir)
    out.mkdir(parents=True, exist_ok=True)
    part = {j: participations(lire_horizon(j, horizon_dir, telecharger), j) for j in ("soumis", "laureats")}
    # Contrôle : participants de TOUS les lauréats Horizon Europe (règle commune) comparés, projet
    # par projet, aux participations de scanR, qui font foi ; alerte si écart (controle_scanr.py)
    from .controle_scanr import controler_horizon_scanr
    controle_scanr = controler_horizon_scanr(part["laureats"], ref_dir,
                                             out_csv=str(Path(horizon_dir) / "horizon_controle_scanr.csv"),
                                             verbose=verbose)
    erc = {j: p[(p["thema_code"] == thema.upper()) | (p["action_id"] == thema.upper())] for j, p in part.items()}
    valeurs = {j: {c: e[c].fillna("(vide)").value_counts().to_dict()
                   for c in ["stage", "erc_role", "destination_code"]} for j, e in erc.items()}
    soumis, laureats = (lignes_pi(erc[j], thema, role_pi) for j in ("soumis", "laureats"))
    projets, soumis, laureats = preparer_projets(soumis, laureats, types_bourse)
    soumis, liens_s = rattacher_structures(soumis, ref_dir)
    laureats, liens_l = rattacher_structures(laureats, ref_dir)
    toutes = pd.concat([soumis, laureats], ignore_index=True)
    # lien (participation -> établissement) avec les attributs de la participation
    attrs = ["ligne", "project_id", "participation_id", "entities_name", "pays_code"]
    liens = {"d": liens_s.merge(soumis[attrs], on="ligne"), "s": liens_l.merge(laureats[attrs], on="ligne")}
    tous_liens = pd.concat(liens.values(), ignore_index=True)

    # Participations sans aucun établissement (ni structure, ni tutelle) : à corriger
    nr = pd.concat([soumis[~soumis["ligne"].isin(liens_s["ligne"])],
                    laureats[~laureats["ligne"].isin(liens_l["ligne"])]], ignore_index=True)
    non_rattaches = non_rattaches_programme(nr, "project_id", "nb_projets", avec_pays=True)
    non_rattaches.to_csv(out / "erc_non_rattaches.csv", sep=";", index=False, encoding="utf-8")

    # Établissements : ROR de la structure, puis tutelles ; pays : pays de la structure
    rors = sorted(set(tous_liens["ror"]))
    lien = (tutelles_ror(rors, data_dir) if rattachement_lignee
            else pd.DataFrame({"ror": rors, "entite_id": rors}))

    def _long(d, src):
        # monde, pays, établissements : règle commune (affiliations.lignes_entites)
        return lignes_entites(d, liens[src], lien, ["project_id", "participation_id"]).assign(src=src)

    # Une ligne par (entité, projet) : déposé dans l'entité (d), signé dans l'entité (s), et
    # nombre de porteurs de chaque côté (programmes.effectifs_projets, commun aux programmes)
    lignes = pd.concat([_long(soumis, "d"), _long(laureats, "s")], ignore_index=True)
    eff = effectifs_projets(lignes, projets, ["annee_appel", "discipline_code", "type_bourse"], pi=True)
    con = duckdb.connect()
    con.register("eff", eff)
    con.execute(f"COPY (SELECT * FROM eff) TO '{out / 'erc_effectifs.parquet'}' (FORMAT parquet)")

    # Contrôle : nb_laureats = nb_candidatures_retenues + arrivées - départs
    ecart = eff["nb_laureats"] - (eff["nb_candidatures_retenues"] + eff["nb_laureats_arrivees"]
                                  - eff["nb_laureats_departs"])
    nb_lignes_ecart = int((ecart != 0).sum())

    # Mobilité des lauréats entre dépôt et signature (structure : ROR, sinon identifiant source)
    def _ensembles(d, col):
        return d.dropna(subset=[col]).groupby("project_id")[col].agg(frozenset)
    cle = lambda d: d.assign(structure=d["ror"].fillna(d["entities_id"]),
                             pays=pays_entite(d["pays_code"]))
    dep_c, sig_c = cle(soumis), cle(laureats)
    mob = pd.DataFrame({"project_id": projets.loc[projets["laureat"] & projets["soumis"], "project_id"]})
    nb_changements = {}
    for col in ["structure", "pays"]:
        dep, sig = mob["project_id"].map(_ensembles(dep_c, col)), mob["project_id"].map(_ensembles(sig_c, col))
        nb_changements[col] = int((dep.notna() & sig.notna() & (dep != sig)).sum())
    laur_sans_cand = projets["laureat"] & ~projets["soumis"]

    # Entités présentes (complètent la collection « entites »)
    cols_tot = ["nb_candidatures", "nb_candidatures_retenues", "nb_laureats"]
    tot = (eff[eff["entite_type"] != "monde"].groupby(["entite_type", "entite_id"])
           [cols_tot].sum().reset_index())
    # (les sommes sur années et panels sont exactes : un projet n'a qu'une année et un panel)
    # nom (Paysage, sinon libellé de la source) et pays (pays des participations) : règle commune
    tot = nommer_entites(tot, tous_liens, lien, ref_dir, pays="source", cle="project_id")
    tot = tot.sort_values(["entite_type", "nb_candidatures"], ascending=[False, False])
    con.register("tot", tot)
    con.execute(f"COPY (SELECT entite_id, entite_type, nom, pays_code, nb_candidatures, "
                f"nb_candidatures_retenues, nb_laureats FROM tot) "
                f"TO '{out / 'erc_entites.parquet'}' (FORMAT parquet)")
    con.close()

    # Pays comparés : liste imposée, ou les N pays qui déposent le plus (France comprise)
    pays_tot = tot[tot["entite_type"] == "pays"].set_index("entite_id")["nb_candidatures"]
    if pays_comparaison:
        compares = list(dict.fromkeys(["FR", *pays_comparaison]))
    else:
        compares = list(dict.fromkeys(["FR", *pays_tot.sort_values(ascending=False)
                                       .head(pays_comparaison_n).index]))

    # Panels inconnus de la bibliométrie (filtre discipline commun)
    f_disc = disciplines_path(nomenclature, data_dir)
    panels = sorted(set(projets["discipline_code"].dropna()))
    hors_biblio = []
    if f_disc.exists():
        disc_biblio = {r[0] for r in duckdb.sql(f"SELECT DISTINCT discipline_code "
                                                f"FROM read_parquet('{f_disc}')").fetchall()}
        hors_biblio = [p for p in panels if p not in disc_biblio]

    nb_dep = soumis["project_id"].nunique()
    dep_rattaches = liens["d"]["project_id"].nunique()
    nb_sig = laureats["project_id"].nunique()
    sig_rattaches = liens["s"]["project_id"].nunique()
    origines = tous_liens["origine"].value_counts().to_dict()
    nb_successeurs = int(toutes["successeur"].sum())
    nb_avec_rnsr = int(toutes["rnsr"].notna().sum())
    nb_avec_tutelles = int((toutes["nb_tutelles_rnsr"] > 0).sum())
    annees = sorted(int(a) for a in projets["annee_appel"].dropna().unique())
    prov, motifs_prov = annees_provisoires(eff, types_principaux, seuil_annee_complete,
                                           annees_provisoires_imposees)
    types_meta = types_bourse_meta(eff, projets, types_principaux)
    monde = eff[eff["entite_type"] == "monde"][EFFECTIFS].sum()
    meta = {
        "source": {"soumis": HORIZON_ODS_SOUMIS, "laureats": HORIZON_ODS_LAUREATS,
                   "date_extraction": dt.date.today().isoformat()},
        "date_calcul": dt.date.today().isoformat(),
        "annees": annees,
        "annees_provisoires": prov,
        "annees_provisoires_motifs": {str(a): m for a, m in motifs_prov.items()},
        "types_bourse": types_meta,
        "types_bourse_defaut": [t for t in types_principaux if t in {x["code"] for x in types_meta}],
        "modes_comptage": {"projets": ["nb_candidatures", "nb_laureats"],
                           "pi": ["nb_pi_candidats", "nb_pi_laureats"]},
        "panels": panels,
        "pays_compares": compares,
        "parametres": {"thema": thema, "role_pi": role_pi, "types_bourse": list(types_bourse or []),
                       "rattachement_lignee": bool(rattachement_lignee),
                       "pays_comparaison": list(pays_comparaison or []),
                       "pays_comparaison_n": pays_comparaison_n,
                       "types_principaux": list(types_principaux),
                       "annees_provisoires_imposees": (None if annees_provisoires_imposees is None
                                                       else list(annees_provisoires_imposees)),
                       "seuil_annee_complete": seuil_annee_complete},
        "controles": {
            "nb_lignes": {j: int(len(p)) for j, p in part.items()},
            "nb_lignes_erc": {j: int(len(e)) for j, e in erc.items()},
            "valeurs": valeurs,
            "nb_candidatures": int(monde["nb_candidatures"]), "nb_laureats": int(monde["nb_laureats"]),
            "nb_pi_candidats": int(monde["nb_pi_candidats"]), "nb_pi_laureats": int(monde["nb_pi_laureats"]),
            "nb_candidatures_rattachees_etablissement": int(dep_rattaches),
            "part_rattachees_etablissement": round(dep_rattaches / nb_dep, 4) if nb_dep else None,
            "part_laureats_rattaches_etablissement": round(sig_rattaches / nb_sig, 4) if nb_sig else None,
            "identifiants": soumis["type_identifiant"].value_counts().to_dict(),
            "identifiants_signature": laureats["type_identifiant"].value_counts().to_dict(),
            "liens_etablissement": origines,
            "nb_participations_successeur": nb_successeurs,
            "nb_participations_avec_rnsr": nb_avec_rnsr,
            "nb_participations_avec_tutelles_rnsr": nb_avec_tutelles,
            "nb_laureats_changement_etablissement": nb_changements["structure"],
            "nb_laureats_changement_pays": nb_changements["pays"],
            "nb_laureats_sans_candidature": int(laur_sans_cand.sum()),
            "nb_lignes_ecart_controle_laureats": nb_lignes_ecart,
            "coherence_scanr_horizon": controle_scanr,
            "nb_structures_non_rattachees": int(len(non_rattaches)),
            "panels_hors_bibliometrie": hors_biblio,
        },
    }
    meta["methode"] = _methode(meta)
    meta["sources"] = (f"Source : open data ESR, jeux {HORIZON_ODS_SOUMIS} et {HORIZON_ODS_LAUREATS} "
                       f"(extraits le {meta['source']['date_extraction']}) ; correspondance Paysage -> ROR.")
    (out / "erc_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")

    if verbose:
        c = meta["controles"]
        print(f"ERC : lignes {c['nb_lignes_erc']} ; {c['nb_candidatures']:,} candidatures "
              f"({c['nb_pi_candidats']:,} PI), {c['nb_laureats']:,} lauréats ({c['nb_pi_laureats']:,} PI), "
              f"taux {c['nb_laureats'] / max(c['nb_candidatures'], 1):.1%}")
        for j, v in valeurs.items():
            print(f"  {j} : {v}")
        print(f"  identifiants de dépôt : {c['identifiants']} ; de signature : {c['identifiants_signature']}")
        print(f"  liens vers un établissement : {origines} ; structures remplacées par leur successeur "
              f"Paysage : {nb_successeurs:,} ; participations avec RNSR : {nb_avec_rnsr:,} "
              f"(dont {nb_avec_tutelles:,} avec des tutelles trouvées)")
        print(f"  rattachés à un établissement (ROR) : candidatures {dep_rattaches:,} / {nb_dep:,} "
              f"({c['part_rattachees_etablissement'] or 0:.1%}), lauréats {sig_rattaches:,} / {nb_sig:,} "
              f"({c['part_laureats_rattaches_etablissement'] or 0:.1%})")
        print(f"  lauréats ayant changé d'établissement entre dépôt et signature : "
              f"{c['nb_laureats_changement_etablissement']:,}, de pays : {c['nb_laureats_changement_pays']:,}")
        if c["nb_laureats_sans_candidature"]:
            print(f"  ATTENTION : {c['nb_laureats_sans_candidature']:,} projets lauréats absents du jeu des "
                  f"soumis (project_id) : comptés comme arrivées, y compris pour MONDE")
        if nb_lignes_ecart:
            print(f"  ATTENTION : {nb_lignes_ecart} lignes où nb_laureats <> nb_candidatures_retenues "
                  "+ nb_laureats_arrivees - nb_laureats_departs")
        fr_nr = non_rattaches[non_rattaches["pays_code"].isin(PAYS_FR)]
        if len(fr_nr):
            print(f"  structures françaises sans ROR : {len(fr_nr)} ({int(fr_nr['nb_projets'].sum())} "
                  f"projets), ex. " + " ; ".join(fr_nr["nom"].head(5).astype(str)))
        if hors_biblio:
            print(f"  panels absents des disciplines de la bibliométrie : {hors_biblio}")
        print("  types de bourse : " + " ; ".join(
            f"{t['code']} {t['nb_candidatures']} cand. / {t['nb_laureats']} laur. "
            f"({t['nb_candidatures_sans_panel']} sans panel)" for t in types_meta))
        print(f"  années provisoires : {motifs_prov or 'aucune'}")
        fr = eff[eff["entite_id"] == "FR"].groupby("annee_appel")[EFFECTIFS].sum()
        fr["taux_succes"] = (fr["nb_laureats"] / fr["nb_candidatures"]).round(3)
        print("  France par année d'appel :\n" + fr.to_string())
        print(f"  pays comparés : {', '.join(compares)}")
        print(f"  -> {out}")
    return {"dossier": str(out), "nb_candidatures": meta["controles"]["nb_candidatures"],
            "nb_laureats": meta["controles"]["nb_laureats"],
            "part_rattachees_etablissement": meta["controles"]["part_rattachees_etablissement"],
            "nb_lignes_effectifs": len(eff)}
