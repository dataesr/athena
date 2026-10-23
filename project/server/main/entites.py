"""
entites.py
==========
Registre des entités (monde, pays, établissements), commun à tous les onglets
et à tous les usages : aperçu HTML (export_dashboard.py) et publication
(publish.py, collection Mongo « entites »).

Trois sources, jointes sur le ROR :
  - OpenAlex (étape 6, indicateurs/<classement>_entites.parquet) : la liste des
    établissements qui ont des articles, leur taille, leurs tutelles ;
  - le référentiel Paysage ou scanR (étape 0, referentiel.py) : nom d'usage,
    sigle, catégorie, ville, et le périmètre du sélecteur ;
  - le fichier de corrections FACULTATIF (config.DASHBOARD_ETABS_PATH) :
    ror ; nom ; alias ; exclure.

Une ligne par entité (construire_entites) :
  entite_id                ROR court, code ISO du pays, ou MONDE
  entite_type              monde | pays | etablissement
  nom                      nom affiché : fichier de corrections > référentiel > OpenAlex
  nom_openalex             nom dans OpenAlex
  sigle                    sigle du référentiel
  categorie                catégorie du référentiel (Université, Organisme de recherche...)
  ville                    ville du référentiel
  referentiel_id           identifiant Paysage (ou scanR)
  pays_code                pays de l'établissement (ou du pays lui-même)
  type_openalex            education, facility, government... (établissements)
  nb_articles              taille (premier périmètre, toute la période)
  pays_compare             pays retenu pour la comparaison internationale
  tutelles                 ROR des tutelles (lignée OpenAlex corrigée)
  nb_structures_rattachees structures dont les articles comptent pour l'entité
  alias                    termes de recherche : alias du fichier, sigle et noms du
                           référentiel, sigles et noms alternatifs OpenAlex, sigle
                           calculé (« Centre National de la Recherche Scientifique » -> CNRS)
  recherche                nom + ROR + alias + ville, en minuscules et sans accents
                           (filtre « contient » côté front, sans traitement)
  selecteur                proposé dans le sélecteur principal
  ordre_selecteur          ordre d'affichage quand la recherche est vide (France = -1)

Sélecteur = (périmètre du référentiel) ∩ (établissements français de l'étape 6)
            + lignes du fichier de corrections sans « exclure »
            - lignes du fichier avec exclure = 1
"""

import io
import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

from .config import (CLASSEMENT, DASHBOARD_ETABS_PATH, DASHBOARD_ORDRE_SELECTEUR, DATA_DIR,
                     REFERENTIEL_DIR, REFERENTIEL_SOURCE)
from .referentiel import charger_referentiel

_MOTS_VIDES = {"de", "du", "des", "la", "le", "les", "l", "d", "et", "en", "pour", "sur", "aux", "au",
               "a", "à", "of", "the", "and", "for", "in", "at", "y", "del", "der", "und", "für"}


def sigle(nom: str):
    """Sigle calculé à partir des initiales des mots significatifs :
    'Centre National de la Recherche Scientifique' -> 'CNRS'. None si < 2 lettres."""
    mots = [m for m in re.split(r"[\s\-'’,()/]+", str(nom or "")) if m]
    init = "".join(m[0] for m in mots if m.lower() not in _MOTS_VIDES and m[0].isalpha())
    return init.upper() if len(init) >= 2 else None


def normaliser(texte: str) -> str:
    """Minuscules, sans accents, apostrophes remplacées par des espaces."""
    t = unicodedata.normalize("NFD", str(texte or ""))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"[’'`]", " ", t).lower()


def load_etab_selection(path: str = DASHBOARD_ETABS_PATH) -> pd.DataFrame:
    """Lit le fichier de corrections du sélecteur (colonnes ror, nom, alias, exclure).

    Format : CSV, séparateur « ; » ou « , », lignes commençant par # ignorées.
      ror    obligatoire : ROR court (02feahw73) ou URL (https://ror.org/02feahw73)
      nom    facultatif : nom affiché (sinon le nom OpenAlex)
      alias  facultatif : termes de recherche supplémentaires, séparés par |
      exclure facultatif : 1 (ou oui) = retirer l'établissement du sélecteur
    Les autres colonnes sont ignorées. L'ordre des lignes sert si
    DASHBOARD_ORDRE_SELECTEUR = "fichier".
    """
    p = Path(path)
    lignes = [l for l in p.read_text(encoding="utf-8-sig").splitlines()
              if l.strip() and not l.lstrip().startswith("#")]
    if not lignes:
        return pd.DataFrame(columns=["ror", "nom", "alias"])
    sep = ";" if lignes[0].count(";") >= lignes[0].count(",") else ","
    df = pd.read_csv(io.StringIO("\n".join(lignes)), sep=sep, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]
    if "ror" not in df.columns:
        raise ValueError(f"{p} : colonne « ror » absente (colonnes lues : {list(df.columns)})")
    for c in ("nom", "alias", "exclure"):
        if c not in df.columns:
            df[c] = ""
    df["ror"] = df["ror"].str.strip().str.rstrip("/").str.split("/").str[-1]
    df = df[df["ror"] != ""].drop_duplicates("ror")
    df = df[["ror", "nom", "alias", "exclure"]].map(lambda v: str(v).strip())
    df["exclure"] = df["exclure"].str.lower().isin(["1", "oui", "true", "vrai", "x"])
    return df


def write_etab_selection(path: str = DASHBOARD_ETABS_PATH, classement: str = CLASSEMENT,
                         data_dir: str = DATA_DIR, min_articles: int = 0,
                         overwrite: bool = False) -> str:
    """Écrit un fichier de corrections de départ avec TOUS les établissements français
    de l'étape 6 (du plus gros au plus petit), colonne exclure vide. Utile sans
    référentiel, ou pour passer en revue le sélecteur ; les lignes sans modification
    peuvent être supprimées. Colonnes : ror ; nom ; alias ; exclure ;
    nb_articles, type_openalex, nom_openalex (pour information, ignorées à la lecture)."""
    p = Path(path)
    if p.exists() and not overwrite:
        raise FileExistsError(f"{p} existe déjà (overwrite=True pour l'écraser)")
    f_ent = Path(data_dir) / "indicateurs" / f"{classement}_entites.parquet"
    df = duckdb.sql(f"""
        SELECT entite_id AS ror, '' AS nom, '' AS alias, '' AS exclure, nb_articles, type_openalex,
               nom AS nom_openalex
        FROM read_parquet('{f_ent}')
        WHERE entite_type = 'etablissement' AND pays_code = 'FR' AND nb_articles >= {int(min_articles)}
        ORDER BY nb_articles DESC
    """).df()
    p.parent.mkdir(parents=True, exist_ok=True)
    entete = ("# Corrections du sélecteur du dashboard (une ligne = un établissement).\n"
              "# nom : nom affiché (vide = nom du référentiel, sinon OpenAlex) ;\n"
              "# alias : termes de recherche séparés par | ; exclure : 1 = retirer du sélecteur.\n"
              "# Une ligne sans exclure ajoute l'établissement au sélecteur.\n"
              "# nb_articles, type_openalex, nom_openalex : pour information, ignorés à la lecture.\n")
    p.write_text(entete + df.to_csv(sep=";", index=False), encoding="utf-8")
    print(f"{p} : {len(df):,} établissements")
    return str(p)


def _alias_openalex(data_dir: Path) -> dict:
    """{ror: (sigles, noms alternatifs)} lus dans institutions.parquet (étape 2)."""
    f = data_dir / "institutions.parquet"
    if not f.exists():
        return {}
    con = duckdb.connect()
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()}
    if "display_name_acronyms" not in cols:
        con.close()
        return {}
    alt = "display_name_alternatives" if "display_name_alternatives" in cols else "[]::VARCHAR[]"
    res = {r[0]: (list(r[1] or []), list(r[2] or [])) for r in con.execute(f"""
        SELECT ror, display_name_acronyms, {alt} FROM read_parquet('{f}')
        WHERE ror IS NOT NULL AND ror <> ''""").fetchall()}
    con.close()
    return res


def construire_entites(classement: str = CLASSEMENT, data_dir: str = DATA_DIR,
                       etabs_path: str = DASHBOARD_ETABS_PATH,
                       referentiel_source: str = REFERENTIEL_SOURCE,
                       referentiel_dir: str = REFERENTIEL_DIR,
                       ordre: str = DASHBOARD_ORDRE_SELECTEUR, verbose: bool = True) -> pd.DataFrame:
    """Registre complet des entités (voir l'en-tête du module).
    etabs_path         : fichier de corrections (None ou absent : pas de corrections)
    referentiel_source : "paysage" ou "scanr" (None : pas de référentiel)
    ordre              : "taille" ou "fichier" (ordre du sélecteur quand la recherche est vide)"""
    data_dir = Path(data_dir)
    f_ent = data_dir / "indicateurs" / f"{classement}_entites.parquet"
    if not f_ent.exists():
        raise FileNotFoundError(f"{f_ent} absent : lancer compute_indicators()")
    ent = duckdb.sql(f"SELECT * FROM read_parquet('{f_ent}')").df()
    ent["nom_openalex"] = ent["nom"]
    etab = ent["entite_type"] == "etablissement"
    fr = etab & (ent["pays_code"] == "FR")

    # --- Référentiel (Paysage ou scanR), joint sur le ROR -----------------------------
    ref = charger_referentiel(referentiel_source, referentiel_dir) if referentiel_source else None
    if ref is None and referentiel_source and verbose:
        print(f"Référentiel {referentiel_source} absent de {referentiel_dir} : lancer build_referentiel() "
              f"(noms OpenAlex, pas de périmètre)")
    cols_ref = ["sigle", "categorie", "ville", "referentiel_id", "nom_officiel", "nom_en", "dans_perimetre"]
    if ref is not None:
        ref = ref.rename(columns={"ror": "entite_id", "nom": "nom_referentiel"})
        ent = ent.merge(ref[["entite_id", "nom_referentiel"] + cols_ref], on="entite_id", how="left")
        # nom affiché : nom d'usage du référentiel pour les établissements qui y sont
        ent["nom"] = ent["nom_referentiel"].where(ent["nom_referentiel"].notna() & etab, ent["nom"])
    else:
        for c in ["nom_referentiel"] + cols_ref:
            ent[c] = None
    ent["dans_perimetre"] = ent["dans_perimetre"].fillna(False).astype(bool)

    # --- Fichier de corrections --------------------------------------------------------
    sel = load_etab_selection(etabs_path) if etabs_path and Path(etabs_path).exists() else None
    if sel is not None:
        noms = {r.ror: r.nom for r in sel.itertuples() if r.nom}
        ent["nom"] = [noms.get(i, n) for i, n in zip(ent["entite_id"], ent["nom"])]

    # --- Alias de recherche ------------------------------------------------------------
    # Ordre de priorité : alias du fichier, sigle et noms du référentiel, sigles et
    # noms alternatifs OpenAlex, sigle calculé ; sans doublon ni répétition du nom.
    oa = _alias_openalex(data_dir)
    cfg = ({r.ror: [a.strip() for a in r.alias.split("|") if a.strip()] for r in sel.itertuples()}
           if sel is not None else {})
    alias = []
    for r in ent.itertuples():
        if r.entite_type != "etablissement":
            alias.append([r.entite_id] if r.entite_type == "pays" else [])
            continue
        sig, alts = oa.get(r.entite_id, ([], []))
        cands = (cfg.get(r.entite_id, []) + [r.sigle, r.nom_officiel, r.nom_en, r.nom_openalex]
                 + sig + [sigle(r.nom)] + [a for a in alts if len(a) <= 80][:6])
        vus, garde = {str(r.nom).lower()}, []
        for a in cands:
            if isinstance(a, str) and a and a.lower() not in vus:
                vus.add(a.lower())
                garde.append(a)
        alias.append(garde)
    ent["alias"] = alias
    # recherche : nom, ROR, alias et ville, en minuscules sans accents
    ent["recherche"] = [normaliser(" ".join([str(n), str(i), *a, v if isinstance(v, str) else ""]))
                        for n, i, a, v in zip(ent["nom"], ent["entite_id"], ent["alias"], ent["ville"])]

    # --- Sélecteur ---------------------------------------------------------------------
    # Base : périmètre du référentiel s'il existe, sinon le fichier, sinon tous les
    # établissements français. Puis ajouts et exclusions du fichier.
    if ref is not None:
        dedans = fr & ent["dans_perimetre"]
    elif sel is not None:
        dedans = fr & ent["entite_id"].isin(set(sel["ror"]))
    else:
        dedans = fr.copy()
    if sel is not None:
        dedans |= fr & ent["entite_id"].isin(set(sel.loc[~sel["exclure"], "ror"]))
        dedans &= ~ent["entite_id"].isin(set(sel.loc[sel["exclure"], "ror"]))
    if ordre == "fichier" and sel is not None:
        rang = {r: k for k, r in enumerate(sel["ror"])}
        cle = ent["entite_id"].map(rang).fillna(len(rang)) * 1e12 - ent["nb_articles"]
    else:
        cle = -ent["nb_articles"].astype(float)
    ent["ordre_selecteur"] = cle.where(dedans).rank(method="first") - 1
    ent.loc[ent["entite_id"] == "FR", "ordre_selecteur"] = -1        # France en tête
    ent["selecteur"] = ent["ordre_selecteur"].notna()
    ent["ordre_selecteur"] = ent["ordre_selecteur"].astype("Int64")

    if verbose:
        print(f"Entités : {len(ent):,} (dont {int(ent['selecteur'].sum()):,} dans le sélecteur)")
        if ref is not None:
            perim = ref[ref["dans_perimetre"]]
            absents = perim[~perim["entite_id"].isin(set(ent.loc[fr, "entite_id"]))]
            print(f"  périmètre {referentiel_source} : {len(perim):,} établissements, dont {len(absents):,} "
                  f"sans entité OpenAlex retenue (moins de ETAB_MIN_ARTICLES articles, ROR absent "
                  f"d'OpenAlex ou exclu)")
            if len(absents):
                print("  exemples : " + " ; ".join(absents["nom_referentiel"].head(8).astype(str)))
        if sel is not None:
            inconnus = sorted(set(sel["ror"]) - set(ent.loc[fr, "entite_id"]))
            if inconnus:
                print(f"  fichier de corrections : {len(inconnus)} ROR sans entité française retenue : "
                      f"{', '.join(inconnus[:20])}{' ...' if len(inconnus) > 20 else ''}")
    return ent.drop(columns=["nom_referentiel", "nom_officiel", "nom_en", "dans_perimetre"])
