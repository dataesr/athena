import pandas as pd
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo.errors import BulkWriteError

from .config import (MONGO_DB, MONGO_URI)

debug = False

MONGO_COLLECTION_NAME = "biblio_projects"
PAYSAGE_URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/paysage_dump.jsonl.gz"
PROJECTS_URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/projects_denormalized.jsonl.gz"


def get_ror(df_paysage, row, field: str = None):
    rors = []
    paysage_ids = row.get(field, [])
    if paysage_ids and isinstance(paysage_ids, list) and len(paysage_ids) > 0:
        for paysage_id in paysage_ids:
            ror = None
            identifiers = df_paysage[df_paysage["id"] == paysage_id]
            if identifiers.size > 0:
                identifiers = identifiers["identifiers"].tolist()
                ror_identifier = [
                    identifier for identifier in identifiers[0] if identifier.get("type") == "ror"]
                if len(ror_identifier) > 0:
                    ror = ror_identifier[0].get("value")
            rors.append(ror)
    row["rors"] = rors
    return row


def chunk_to_records(df_chunk):
    """Convertit un chunk en liste de dicts, avec NaN/NaT remplacés par None."""
    return (
        df_chunk.astype(object)
        .where(df_chunk.notna(), None)
        .to_dict(orient="records")
    )


def insert_chunk(collection, df_chunk):
    """Convertit puis insère un chunk. Retourne le nombre de documents insérés."""
    records = chunk_to_records(df_chunk)
    try:
        result = collection.insert_many(records, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        # Avec ordered=False, les documents valides sont quand même insérés
        return bwe.details.get("nInserted", 0)


def insert_dataframe_parallel(df, collection, chunk_size=5000, max_workers=8):
    # iloc crée des vues légères : la conversion coûteuse se fait dans les workers
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    total_inserted = 0
    errors = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(insert_chunk, collection, chunk): idx
            for idx, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                total_inserted += future.result()
            except Exception as e:
                errors.append((idx, e))
                print(f"Erreur sur le chunk {idx} : {e}")
    print(f"{total_inserted} records added successfully "
          f"({len(chunks)} chunks, {len(errors)} en erreur)")
    return total_inserted, errors


def generate_projects():
  df_projects = pd.DataFrame()
  chunksize = 5 if debug else 1000
  with pd.read_json(PROJECTS_URL, lines=True, chunksize=chunksize) as reader:
      index = 0
      for chunk in reader:
          if debug:
              print(f"index: {index}")
          index = index + 1
          if debug and index > 5:
              break
          df_projects = pd.concat([df_projects, chunk])
  df_paysage = pd.read_json(PAYSAGE_URL, lines=True)
  df_projects = df_projects.apply(lambda x: get_ror(df_paysage, x, "participants_id_search"), axis=1)
  client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
  db = client[MONGO_DB]
  collection = db[MONGO_COLLECTION_NAME]
  insert_dataframe_parallel(df_projects, collection, chunk_size=5000, max_workers=8)