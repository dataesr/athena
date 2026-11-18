"""
programmes.py
=============
Code commun aux programmes de financement et de distinction (bourses ERC, ANR, prix, puis
piliers 2-3 d'Horizon Europe et candidatures ANR) : ce qui suit le rattachement aux
établissements (affiliations.py).

1. effectifs_projets(lignes, projets, dims)   candidatures, lauréats, arrivées / départs, et en
   option porteurs (PI), coordination, montants, par entité et dimensions (année, panel...).
   La candidature compte dans la structure de dépôt (src = "d"), le lauréat dans la structure
   de signature (src = "s") ; un programme sans candidatures n'a que des lignes "s".
2. nommer_entites(tot, liens, lien)            nom et pays des entités présentes :
   nom   : référentiel Paysage, sinon libellé le plus fréquent dans la source, sinon l'identifiant ;
           pays : PAYS_NOMS ;
   pays  : "source" (pays le plus fréquent des participations de l'établissement, sinon de ses
           structures, pour une tutelle sans participation directe) ou "referentiel" (pays
           Paysage de l'établissement).
3. non_rattaches(nr, ...)                      structures sans établissement, à corriger dans
   les référentiels (une ligne par identifiant, nombre de projets ou de lauréats touchés).
"""

import duckdb
import pandas as pd

from .common import PAYS_NOMS, plus_frequent, texte
from .referentiel import charger_correspondance_paysage


def effectifs_projets(lignes: pd.DataFrame, projets: pd.DataFrame, dims: list, pi: bool = True,
                      coordination: bool = False, montant: bool = False) -> pd.DataFrame:
    """Effectifs par (entite_type, entite_id, *dims).
    lignes  : entite_type, entite_id, project_id, participation_id, src ("d" dépôt / "s"
              signature), et coordinateur (bool) / montant si demandés ;
    projets : project_id, laureat (bool), *dims.
    Colonnes : nb_candidatures (déposés dans l'entité), nb_candidatures_retenues (dont financés,
    où qu'ils soient signés), nb_laureats (signés dans l'entité), nb_laureats_arrivees (signés,
    non déposés dans l'entité), nb_laureats_departs (déposés, financés, signés ailleurs) ;
    pi : nb_pi_candidats, nb_pi_laureats (participations distinctes de chaque côté) ;
    coordination : nb_laureats_coordination (lauréats coordonnés par l'entité) ;
    montant : montant_laureats (somme des montants des participations signées dans l'entité)."""
    extra_pp = ""
    extra = ""
    if pi:
        extra += (",\n               sum(pi_d)::BIGINT AS nb_pi_candidats,"
                  "\n               sum(pi_s)::BIGINT AS nb_pi_laureats")
    if coordination:
        extra_pp += ",\n                   bool_or(src = 's' AND coordinateur) AS coord_s"
        extra += ",\n               count(*) FILTER (WHERE s AND coord_s)::BIGINT AS nb_laureats_coordination"
    if montant:
        extra_pp += ",\n                   sum(montant) FILTER (WHERE src = 's') AS montant_s"
        extra += ",\n               sum(montant_s) AS montant_laureats"
    dims_sql = ", ".join(f"p.{d}" for d in dims)
    n = 2 + len(dims)
    with duckdb.connect() as con:
        con.register("lignes", lignes)
        con.register("projets", projets)
        return con.execute(f"""
            WITH par_projet AS (
                SELECT entite_type, entite_id, project_id,
                       bool_or(src = 'd') AS d, bool_or(src = 's') AS s,
                       count(DISTINCT participation_id) FILTER (WHERE src = 'd') AS pi_d,
                       count(DISTINCT participation_id) FILTER (WHERE src = 's') AS pi_s{extra_pp}
                FROM lignes GROUP BY ALL)
            SELECT c.entite_type, c.entite_id, {dims_sql},
                   count(*) FILTER (WHERE d)::BIGINT AS nb_candidatures,
                   count(*) FILTER (WHERE d AND p.laureat)::BIGINT AS nb_candidatures_retenues,
                   count(*) FILTER (WHERE s)::BIGINT AS nb_laureats,
                   count(*) FILTER (WHERE s AND NOT d)::BIGINT AS nb_laureats_arrivees,
                   count(*) FILTER (WHERE d AND p.laureat AND NOT s)::BIGINT AS nb_laureats_departs{extra}
            FROM par_projet c JOIN projets p USING (project_id)
            GROUP BY {", ".join(str(i) for i in range(1, n + 1))}
            ORDER BY CASE entite_type WHEN 'monde' THEN 0 WHEN 'pays' THEN 1 ELSE 2 END,
                     {", ".join(str(i) for i in range(2, n + 1))}
        """).df()


def nommer_entites(tot: pd.DataFrame, liens: pd.DataFrame, lien: pd.DataFrame, ref_dir: str,
                   pays: str = "source", cle: str = "project_id") -> pd.DataFrame:
    """Ajoute nom et pays_code à tot (entite_type, entite_id, ...).
    liens : (ror, origine, entities_name[, pays_code, cle]) : liens participation -> établissement
            de affiliations.rattacher_structures, avec les attributs de la participation ;
    lien  : (ror, entite_id) de affiliations.tutelles_ror.
    pays  : "source" ou "referentiel" (voir l'en-tête)."""
    tot = tot.copy()
    corresp = charger_correspondance_paysage(ref_dir)
    c1 = corresp.drop_duplicates("ror").set_index("ror")
    noms_ref = c1["nom"].to_dict()
    noms_src = (liens[liens["origine"] == "structure"].groupby("ror")["entities_name"]
                .agg(plus_frequent).to_dict())
    etab = tot["entite_type"] == "etablissement"
    tot["nom"] = [noms_ref.get(i) or noms_src.get(i) or i if e else PAYS_NOMS.get(i, i)
                  for i, e in zip(tot["entite_id"], etab)]
    if pays == "referentiel":
        from .horizon import pays_iso2
        pays_ref = c1["pays_iso3"].map(lambda v: pays_iso2(texte(v))).to_dict()
        tot["pays_code"] = tot["entite_id"].map(pays_ref).where(etab, tot["entite_id"])
    elif pays == "source":
        pays_src = liens.groupby("ror")["pays_code"].agg(plus_frequent).to_dict()
        # tutelle sans ligne directe : pays le plus fréquent de ses structures
        pays_tut = (liens[[cle, "ror"]].drop_duplicates().merge(lien, on="ror")
                    .assign(pays=lambda x: x["ror"].map(pays_src))
                    .groupby("entite_id")["pays"].agg(plus_frequent).to_dict())
        tot["pays_code"] = [(pays_src.get(i) or pays_tut.get(i)) if e else i
                            for i, e in zip(tot["entite_id"], etab)]
    else:
        raise ValueError(f"pays : 'source' ou 'referentiel', pas {pays!r}")
    return tot


def non_rattaches(nr: pd.DataFrame, col_compte: str, nom_compte: str, nom: str = "frequent",
                  avec_pays: bool = False, cle_nom_si_vide: bool = False) -> pd.DataFrame:
    """Structures sans établissement : une ligne par (entities_id, type_identifiant), avec le
    nom (« frequent » : le plus fréquent ; « premier » : le premier renseigné), le pays le plus
    fréquent (avec_pays) et nom_compte = nombre de col_compte distincts (projets, lauréats),
    du plus touché au moins touché.
    cle_nom_si_vide : partenaires sans identifiant regroupés par nom (colonne cle en tête)."""
    agg = {"nom": ("entities_name", plus_frequent if nom == "frequent"
                   else (lambda v: v.dropna().iloc[0] if v.notna().any() else None))}
    if avec_pays:
        agg["pays_code"] = ("pays_code", plus_frequent)
    agg[nom_compte] = (col_compte, "nunique")
    cles = ["entities_id", "type_identifiant"]
    if cle_nom_si_vide:
        nr = nr.assign(entities_id=nr["entities_id"].fillna("(sans identifiant)"))
        nr = nr.assign(cle=nr["entities_id"].where(nr["type_identifiant"] != "vide", nr["entities_name"]))
        cles = ["cle"] + cles
    return (nr.groupby(cles, dropna=False).agg(**agg).reset_index()
            .sort_values(nom_compte, ascending=False))
