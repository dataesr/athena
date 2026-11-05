"""
prix.py
=======
Prix et distinctions scientifiques (Institut universitaire de France, médailles du CNRS,
prix des académies et des sociétés savantes...) par établissement, année et prix, pour le
tableau suivi-recherche. Mêmes clés que la bibliométrie et les ERC (CONTRAT_DONNEES.md) :
entite_id = ROR court des établissements, FR pour l'ensemble.

Source
------
  Jeu ODS PRIX_ODS (fr_esr_paysage_laureats_all, non public : clé ODS_API_KEY) : lauréats
  des prix recensés dans Paysage, une ligne par lauréat d'un prix une année
  (prix_laureat_identifiant). Le lauréat est une personne (laureat_type = Personne), avec
  ses structures d'affiliation (laureat_structures_associees_identifiant : identifiants
  Paysage, plusieurs séparés par ;), ou une structure (laureat_type = Structure, ex.
  entreprises du concours i-Nov).

Règles
------
  Année : colonne annee, à défaut les 4 premiers chiffres de prix_annee (2012, 1975-01-01,
  2020-02...). Lauréats sans année écartés (nombre dans prix_meta.controles).
  Établissement (même logique que les ERC, scanR) : chaque structure d'affiliation
  (Paysage, et ROR des structures), remplacée par son successeur Paysage (établissement
  actuel), et toutes les tutelles de ses laboratoires RNSR
  (laureat_structures_associees_identifiant_rnsr) ; pour un lauréat Structure, la structure
  elle-même. Si PRIX_RATTACHEMENT_LIGNEE, aussi les tutelles OpenAlex de ces établissements.
  Les structures sans établissement sont listées dans prix_non_rattaches.csv.
  Comptage entier : un lauréat compte 1 pour chacun de ses établissements, une seule fois
  quel que soit le nombre de chemins ; FR compte chaque lauréat une fois (tous les lauréats
  du jeu, qui recense les prix du système français).
  Les effectifs comptent des attributions (prix_laureat_identifiant) : une personne nommée
  membre junior puis senior de l'IUF compte deux fois dans le groupe IUF.

Sorties (dans PRIX_DIR = <DATA_DIR>/prix/)
------------------------------------------
  prix_effectifs.parquet    une ligne par entite_type x entite_id x annee x prix_id :
                            nb_laureats, nb_laureats_femmes, nb_laureats_hommes
  prix_catalogue.parquet    une ligne par prix : libellés, catégories, porteurs, site,
                            effectifs et années, groupes (PRIX_GROUPES)
  prix_laureats.parquet     une ligne par lauréat : identité, prix, année, structures,
                            établissements (entites)
  prix_entites.parquet      FR et établissements présents : entite_id, entite_type, nom,
                            pays_code, nb_laureats, toutes années (complète la collection
                            « entites », entites.py)
  prix_meta.json            source, années, groupes, contrôles, textes de méthode
  prix_non_rattaches.csv    structures d'affiliation sans établissement (à corriger dans
                            Paysage)
  prix_personnes.parquet    usage interne (prix_disciplines.py) : lauréats, nom, ORCID,
                            établissements, même si PRIX_PUBLIER_LAUREATS = False
  prix_brut.parquet         copie brute du jeu (telecharger=False la relit)

Exemple :
    from project.server.main.prix import compute_prix
    res = compute_prix()
    res = compute_prix(telecharger=False)   # réutilise prix_brut.parquet
"""

import datetime as dt
import json
import re
from pathlib import Path

import duckdb
import pandas as pd

from .config import (DATA_DIR, PRIX_ANNEE_MIN, PRIX_COLONNES_EXCLUES, PRIX_DIR, PRIX_GROUPES,
                     PRIX_ODS, PRIX_PUBLIER_LAUREATS, PRIX_RATTACHEMENT_LIGNEE, REFERENTIEL_DIR)
from .erc import tutelles_ror
from .horizon import pays_iso2, rattacher_structures
from .referentiel import charger_correspondance_paysage
from .utils_ods import get_ods_data

ENTITE_NATIONALE = "FR"
COLONNES_REQUISES = ["prix_laureat_identifiant", "prix_identifiant", "laureat_type"]
EFFECTIFS = ["nb_laureats", "nb_laureats_femmes", "nb_laureats_hommes"]


def prix_path(nom: str, prix_dir: str = PRIX_DIR) -> Path:
    return Path(prix_dir) / nom


def lire_prix(prix_dir: str = PRIX_DIR, telecharger: bool = True,
              colonnes_exclues=PRIX_COLONNES_EXCLUES, jeu: str = PRIX_ODS) -> pd.DataFrame:
    """Jeu brut en texte, sans `colonnes_exclues`, copié dans <prix_dir>/prix_brut.parquet.
    telecharger=False : relit cette copie."""
    f = prix_path("prix_brut.parquet", prix_dir)
    if not telecharger:
        if not f.exists():
            raise FileNotFoundError(f"{f} absent : relancer avec telecharger=True")
        return duckdb.sql(f"SELECT * FROM read_parquet('{f}')").df()
    exclues = {c.lower() for c in (colonnes_exclues or [])}
    df = get_ods_data(jeu, strict=True, dtype=str, keep_default_na=False,
                      usecols=lambda c: c.lstrip("﻿").strip().lower() not in exclues)
    if df.empty:
        raise ValueError(f"jeu ODS {jeu} vide")
    df.columns = [c.lstrip("﻿").strip().lower() for c in df.columns]   # BOM éventuel
    f.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as con:
        con.register("df", df)
        con.execute(f"COPY df TO '{f}' (FORMAT parquet, COMPRESSION zstd)")
    return df


# --- Normalisation -----------------------------------------------------------------------

def _texte(v):
    v = str(v).strip() if v is not None and not (isinstance(v, float) and pd.isna(v)) else ""
    return v or None


def _liste(v) -> list:
    """Valeurs séparées par ; (sans vides, ordre conservé)."""
    v = _texte(v)
    return [x.strip() for x in v.split(";") if x.strip()] if v else []


def _annee(annee, prix_annee):
    for v in (annee, prix_annee):
        m = re.match(r"^\s*(\d{4})", str(v or ""))
        if m:
            return int(m.group(1))
    return None


def _genre(v):
    v = (_texte(v) or "").lower()
    return {"femme": "femme", "homme": "homme"}.get(v)


def laureats(df: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par lauréat (prix_laureat_identifiant), colonnes normalisées :
    laureat_id, prix_id, annee, laureat_type, personne_id, libelle, prenom, nom, genre
    (femme | homme | None), structures_ids, structures_libelles (listes), structures_ror,
    rnsr (listes : identifiants des structures, à rattacher), idref, orcid, wikidata."""
    manquantes = [c for c in COLONNES_REQUISES if c not in df.columns]
    if manquantes:
        raise ValueError(f"colonnes absentes du jeu des prix : {manquantes} (lues : {list(df.columns)})")
    col = lambda c: (df[c].map(_texte) if c in df.columns else pd.Series([None] * len(df), index=df.index))
    lst = lambda c: (df[c].map(_liste) if c in df.columns else pd.Series([[]] * len(df), index=df.index))
    l = pd.DataFrame({
        "laureat_id": col("prix_laureat_identifiant"),
        "prix_id": col("prix_identifiant"),
        "annee": [_annee(a, b) for a, b in zip(col("annee"), col("prix_annee"))],
        "laureat_type": col("laureat_type"),
        "personne_id": col("laureat_identifiant"),
        "libelle": col("laureat_libelle"),
        "prenom": col("laureat_personne_prenom"),
        "nom": col("laureat_personne_nom"),
        "genre": col("laureat_personne_genre").map(_genre),
        "structures_ids": lst("laureat_structures_associees_identifiant"),
        "structures_libelles": lst("laureat_structures_associees_libelle"),
        "structures_ror": lst("laureat_structures_associees_identifiant_ror"),
        "rnsr": lst("laureat_structures_associees_identifiant_rnsr"),
        "idref": col("laureat_identifiant_idref"),
        "orcid": col("laureat_identifiant_orcid"),
        "wikidata": col("laureat_identifiant_wikidata"),
        # identifiants propres du lauréat (lauréat Structure)
        "ror_propre": lst("laureat_identifiant_ror"),
        "rnsr_propre": lst("laureat_identifiant_rnsr"),
    })
    l["annee"] = l["annee"].astype("Int64")
    l = l[l["laureat_id"].notna() & l["prix_id"].notna()]
    return l.drop_duplicates("laureat_id").reset_index(drop=True)


def catalogue_prix(df: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par prix : prix_id, libelle, libelle_francais, libelle_anglais,
    categories, porteurs_ids, porteurs_libelles (listes), site_internet, wikidata."""
    col = lambda c: (df[c].map(_texte) if c in df.columns else pd.Series([None] * len(df), index=df.index))
    p = pd.DataFrame({
        "prix_id": col("prix_identifiant"), "libelle": col("prix_libelle"),
        "libelle_francais": col("prix_libelle_francais"), "libelle_anglais": col("prix_libelle_anglais"),
        "categories": col("prix_categories"), "porteurs_ids": col("prix_porteurs_identifiant"),
        "porteurs_libelles": col("prix_porteurs_libelle"), "site_internet": col("prix_site_internet"),
        "wikidata": col("prix_identifiant_wikidata"),
    }).dropna(subset=["prix_id"])

    def _premier(s):
        s = s.dropna()
        return s.value_counts().index[0] if len(s) else None

    cat = p.groupby("prix_id").agg({c: _premier for c in p.columns if c != "prix_id"}).reset_index()
    for c in ["categories", "porteurs_ids", "porteurs_libelles"]:
        cat[c] = cat[c].map(_liste)
    cat["site_internet"] = cat["site_internet"].map(lambda v: _liste(v)[0] if _liste(v) else None)
    return cat


def groupes_prix(cat: pd.DataFrame, groupes=PRIX_GROUPES) -> dict:
    """{code du groupe: [prix_id]} selon PRIX_GROUPES (prix, porteurs ou catégories)."""
    res = {}
    for g in groupes or []:
        ids, porteurs, cats = set(g.get("prix") or []), set(g.get("porteurs") or []), set(g.get("categories") or [])
        dedans = (cat["prix_id"].isin(ids) | cat["porteurs_ids"].map(lambda v: bool(porteurs & set(v)))
                  | cat["categories"].map(lambda v: bool(cats & set(v))))
        res[g["code"]] = sorted(cat.loc[dedans, "prix_id"])
    return res


def structures_a_rattacher(l: pd.DataFrame) -> pd.DataFrame:
    """Une ligne par (lauréat, identifiant de structure), au format de
    horizon.rattacher_structures : laureat_id, entities_id, entities_name, rnsr. Les RNSR du
    lauréat sont sur sa première ligne ; un lauréat sans structure garde une ligne vide."""
    lignes = []
    for r in l.itertuples():
        # identifiants et libellés ne sont pas dans le même ordre : nom seulement si une structure
        noms = ({r.structures_ids[0]: r.structures_libelles[0]}
                if len(r.structures_ids) == 1 and len(r.structures_libelles) == 1 else {})
        ids = list(r.structures_ids) + list(r.structures_ror)
        rnsr = list(r.rnsr)
        if r.laureat_type == "Structure":
            ids += [r.personne_id] if r.personne_id else []
            ids += list(r.ror_propre)
            rnsr += list(r.rnsr_propre)
            noms.setdefault(r.personne_id, r.libelle)
        ids = list(dict.fromkeys(i for i in ids if i)) or [None]
        for k, i in enumerate(ids):
            lignes.append((r.laureat_id, i, noms.get(i), ";".join(rnsr) if k == 0 and rnsr else None))
    return pd.DataFrame(lignes, columns=["laureat_id", "entities_id", "entities_name", "rnsr"])


# --- Calcul ------------------------------------------------------------------------------

def _methode(meta: dict) -> list:
    a = meta["annees"]
    groupes = " ; ".join(f"{g['libelle']} ({g['nb_prix']} prix)" for g in meta["groupes"])
    return [
        "Source : open data de l'enseignement supérieur et de la recherche, lauréats des prix et "
        f"distinctions recensés dans Paysage (« {meta['source']['jeu']} »).",
        (f"Couverture : années {a[0]} à {a[-1]}, année d'attribution du prix." if a
         else "Couverture : aucune année."),
        "Établissement : chaque structure d'affiliation du lauréat, remplacée par l'établissement "
        "actuel si elle a fusionné (successeurs Paysage), et toutes les tutelles de ses laboratoires "
        "(RNSR, comme scanR)"
        + (", puis les tutelles de ces établissements (lignée OpenAlex)" if meta["parametres"]["rattachement_lignee"]
           else "") + ".",
        "Comptage entier : un lauréat compte pour chacun de ses établissements, une seule fois ; "
        "la France compte chaque lauréat une fois. On compte des attributions : une personne "
        "distinguée deux fois (membre junior puis senior de l'IUF) compte deux fois.",
        f"Groupes de prix : {groupes}." if groupes else "Aucun groupe de prix défini.",
        "Le recensement de Paysage n'est pas exhaustif, surtout pour les années anciennes.",
    ]


def compute_prix(prix_dir: str = PRIX_DIR, data_dir: str = DATA_DIR, ref_dir: str = REFERENTIEL_DIR,
                 rattachement_lignee: bool = PRIX_RATTACHEMENT_LIGNEE, annee_min=PRIX_ANNEE_MIN,
                 groupes=PRIX_GROUPES, publier_laureats: bool = PRIX_PUBLIER_LAUREATS,
                 telecharger: bool = True, verbose: bool = True) -> dict:
    """Calcule les effectifs des prix et écrit les sorties dans prix_dir (voir l'en-tête).
    telecharger=False : relit la copie brute. Renvoie un résumé."""
    out = Path(prix_dir)
    out.mkdir(parents=True, exist_ok=True)
    brut = lire_prix(prix_dir, telecharger)
    l_tous = laureats(brut)
    cat = catalogue_prix(brut)

    sans_annee = l_tous["annee"].isna()
    avant = l_tous["annee"].notna() & (l_tous["annee"] < annee_min) if annee_min else pd.Series(False, index=l_tous.index)
    l = l_tous[~sans_annee & ~avant].reset_index(drop=True)

    # Établissements de chaque lauréat
    s = structures_a_rattacher(l)
    s, liens = rattacher_structures(s, ref_dir)
    liens = liens.merge(s[["ligne", "laureat_id", "entities_id", "entities_name"]], on="ligne")
    rors = sorted(set(liens["ror"]))
    lien = (tutelles_ror(rors, data_dir) if rattachement_lignee
            else pd.DataFrame({"ror": rors, "entite_id": rors}))
    etabs = liens[["laureat_id", "ror"]].merge(lien, on="ror")[["laureat_id", "entite_id"]].drop_duplicates()

    # Effectifs : FR (tous les lauréats) + établissements
    attrs = l[["laureat_id", "annee", "prix_id", "genre"]]
    long = pd.concat([attrs.assign(entite_type="pays", entite_id=ENTITE_NATIONALE),
                      etabs.merge(attrs, on="laureat_id").assign(entite_type="etablissement")],
                     ignore_index=True)
    con = duckdb.connect()
    con.register("long", long)
    eff = con.execute("""
        SELECT entite_type, entite_id, annee::INTEGER AS annee, prix_id,
               count(DISTINCT laureat_id)::BIGINT AS nb_laureats,
               count(DISTINCT laureat_id) FILTER (WHERE genre = 'femme')::BIGINT AS nb_laureats_femmes,
               count(DISTINCT laureat_id) FILTER (WHERE genre = 'homme')::BIGINT AS nb_laureats_hommes
        FROM long GROUP BY 1, 2, 3, 4
        ORDER BY CASE entite_type WHEN 'pays' THEN 0 ELSE 1 END, 2, 3, 4
    """).df()
    con.register("eff", eff)
    con.execute(f"COPY eff TO '{out / 'prix_effectifs.parquet'}' (FORMAT parquet)")

    # Catalogue : effectifs nationaux, années, groupes
    gp = groupes_prix(cat, groupes)
    fr = eff[eff["entite_id"] == ENTITE_NATIONALE]
    par_prix = fr.groupby("prix_id").agg(nb_laureats=("nb_laureats", "sum"),
                                         nb_laureats_femmes=("nb_laureats_femmes", "sum"),
                                         nb_laureats_hommes=("nb_laureats_hommes", "sum"),
                                         annee_min=("annee", "min"), annee_max=("annee", "max"))
    cat = cat.merge(par_prix, left_on="prix_id", right_index=True, how="left")
    for c in EFFECTIFS:
        cat[c] = cat[c].fillna(0).astype("int64")
    for c in ["annee_min", "annee_max"]:
        cat[c] = cat[c].astype("Int64")
    cat["groupes"] = [[g for g, ids in gp.items() if p in ids] for p in cat["prix_id"]]
    cat = cat.sort_values(["nb_laureats", "prix_id"], ascending=[False, True]).reset_index(drop=True)
    con.register("cat", cat)
    con.execute(f"COPY cat TO '{out / 'prix_catalogue.parquet'}' (FORMAT parquet)")

    # Lauréats (liste nominative), avec leurs établissements
    ent_l = etabs.groupby("laureat_id")["entite_id"].agg(lambda v: sorted(set(v)))
    lib_prix = cat.set_index("prix_id")["libelle"]
    pub = l.drop(columns=["ror_propre", "rnsr_propre", "structures_ror", "rnsr"]).copy()
    pub["prix_libelle"] = pub["prix_id"].map(lib_prix)
    pub["entites"] = pub["laureat_id"].map(ent_l).map(lambda v: v if isinstance(v, list) else [])
    pub = pub[["laureat_id", "prix_id", "prix_libelle", "annee", "laureat_type", "personne_id", "libelle",
               "prenom", "nom", "genre", "structures_ids", "structures_libelles", "idref", "orcid",
               "wikidata", "entites"]].sort_values(["annee", "prix_id", "nom"], na_position="last")
    f_laur = out / "prix_laureats.parquet"
    if publier_laureats:
        con.register("pub", pub)
        con.execute(f"COPY pub TO '{f_laur}' (FORMAT parquet)")
    elif f_laur.exists():
        f_laur.unlink()

    # Personnes et établissements, toujours écrit (usage interne : prix_disciplines.py)
    pers = l[["laureat_id", "prix_id", "annee", "laureat_type", "prenom", "nom", "orcid"]].copy()
    pers["entites"] = pers["laureat_id"].map(ent_l).map(lambda v: v if isinstance(v, list) else [])
    con.register("pers", pers)
    con.execute(f"COPY pers TO '{out / 'prix_personnes.parquet'}' (FORMAT parquet)")

    # Entités : nom et pays (référentiel Paysage, sinon libellé de la structure)
    corresp = charger_correspondance_paysage(ref_dir)
    c1 = corresp.drop_duplicates("ror").set_index("ror")
    noms_ref, pays_ref = c1["nom"].to_dict(), c1["pays_iso3"].map(lambda v: pays_iso2(_texte(v))).to_dict()
    noms_src = (liens[liens["origine"] == "structure"].dropna(subset=["entities_name"])
                .groupby("ror")["entities_name"].agg(lambda v: v.value_counts().index[0]).to_dict())
    tot = (eff.groupby(["entite_type", "entite_id"])["nb_laureats"].sum().reset_index())
    etab = tot["entite_type"] == "etablissement"
    tot["nom"] = [noms_ref.get(i) or noms_src.get(i) or i if e else "France"
                  for i, e in zip(tot["entite_id"], etab)]
    tot["pays_code"] = tot["entite_id"].map(pays_ref).where(etab, ENTITE_NATIONALE)
    tot = tot[["entite_id", "entite_type", "nom", "pays_code", "nb_laureats"]]
    con.register("tot", tot)
    con.execute(f"COPY tot TO '{out / 'prix_entites.parquet'}' (FORMAT parquet)")
    con.close()

    # Structures sans établissement : à corriger dans Paysage
    s_ok = set(liens["ligne"])
    nr = s[~s["ligne"].isin(s_ok) & s["entities_id"].notna()]
    nr = nr[~nr["laureat_id"].isin(set(etabs["laureat_id"]))]
    non_rattaches = (nr.groupby(["entities_id", "type_identifiant"], dropna=False)
                     .agg(nom=("entities_name", lambda v: v.dropna().iloc[0] if v.notna().any() else None),
                          nb_laureats=("laureat_id", "nunique"))
                     .reset_index().sort_values("nb_laureats", ascending=False))
    non_rattaches.to_csv(out / "prix_non_rattaches.csv", sep=";", index=False, encoding="utf-8")

    # Méta
    annees = sorted(int(a) for a in eff["annee"].dropna().unique())
    personnes = l["laureat_type"] == "Personne"
    sans_structure = personnes & l["structures_ids"].map(len).eq(0) & l["rnsr"].map(len).eq(0)
    rattaches = l["laureat_id"].isin(set(etabs["laureat_id"]))
    nb_fr = cat.set_index("prix_id")["nb_laureats"]
    meta = {
        "source": {"jeu": PRIX_ODS, "date_extraction": dt.date.today().isoformat()},
        "date_calcul": dt.date.today().isoformat(),
        "annees": list(range(annees[0], annees[-1] + 1)) if annees else [],
        "groupes": [{"code": g["code"], "libelle": g.get("libelle", g["code"]), "prix": gp[g["code"]],
                     "nb_prix": len(gp[g["code"]]),
                     "nb_laureats": int(nb_fr.reindex(gp[g["code"]]).fillna(0).sum())}
                    for g in (groupes or [])],
        "groupe_defaut": (groupes or [{}])[0].get("code"),
        "nb_prix": int(len(cat)),
        "parametres": {"rattachement_lignee": rattachement_lignee, "annee_min": annee_min,
                       "publier_laureats": publier_laureats},
        "controles": {
            "nb_lignes": int(len(brut)),
            "nb_laureats": int(len(l_tous)),
            "nb_lignes_doublons": int(len(brut) - len(l_tous)),
            "nb_laureats_sans_annee": int(sans_annee.sum()),
            "nb_laureats_avant_annee_min": int(avant.sum()),
            "laureat_type": l["laureat_type"].fillna("(vide)").value_counts().to_dict(),
            "genre": l["genre"].fillna("(inconnu)").value_counts().to_dict(),
            "nb_personnes_sans_structure": int(sans_structure.sum()),
            "nb_laureats_rattaches_etablissement": int(rattaches.sum()),
            "part_laureats_avec_structure_rattaches": round(
                float(rattaches[~sans_structure].mean()) if (~sans_structure).any() else 0.0, 4),
            "liens_etablissement": liens["origine"].value_counts().to_dict(),
            "nb_structures_successeur": int(s["successeur"].sum()),
            "nb_laureats_avec_rnsr": int(l["rnsr"].map(len).gt(0).sum()),
            "nb_laureats_avec_tutelles_rnsr": int(s.loc[s["nb_tutelles_rnsr"] > 0, "laureat_id"].nunique()),
            "nb_etablissements": int(etab.sum()),
            "nb_etablissements_fr": int((etab & (tot["pays_code"] == "FR")).sum()),
            "groupes_vides": [g for g, ids in gp.items() if not ids],
        },
    }
    meta["methode"] = _methode(meta)
    meta["sources"] = (f"Source : open data ESR, jeu {PRIX_ODS} (lauréats des prix recensés dans Paysage), "
                       f"extrait le {meta['source']['date_extraction']} ; rattachement : Paysage, RNSR "
                       f"(scanR)" + (", lignée OpenAlex" if rattachement_lignee else "") + ".")
    (out / "prix_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=1), encoding="utf-8")

    if verbose:
        c = meta["controles"]
        print(f"Prix : {c['nb_laureats']:,} lauréats, {meta['nb_prix']:,} prix, années "
              f"{annees[0] if annees else '-'}-{annees[-1] if annees else '-'}")
        print(f"  rattachés à un établissement : {c['nb_laureats_rattaches_etablissement']:,} "
              f"({c['part_laureats_avec_structure_rattaches']:.1%} des lauréats avec structure) ; "
              f"liens {c['liens_etablissement']} ; {c['nb_structures_successeur']:,} structures "
              f"remplacées par leur successeur")
        print(f"  sans année : {c['nb_laureats_sans_annee']:,} ; personnes sans structure : "
              f"{c['nb_personnes_sans_structure']:,}")
        for g in meta["groupes"]:
            print(f"  groupe {g['code']} : {g['nb_prix']} prix, {g['nb_laureats']:,} lauréats")
        if c["groupes_vides"]:
            print(f"  ATTENTION : groupes sans prix : {', '.join(c['groupes_vides'])} (PRIX_GROUPES)")
        if len(non_rattaches):
            print(f"  {len(non_rattaches):,} structures sans établissement -> {out / 'prix_non_rattaches.csv'}")
    return {"nb_laureats": int(len(l)), "nb_lignes_effectifs": int(len(eff)), "nb_prix": int(len(cat)),
            "annees": meta["annees"], "controles": meta["controles"]}
