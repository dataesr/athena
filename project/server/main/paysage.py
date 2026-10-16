import pandas as pd
url_paysage = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/paysage_dump.jsonl.gz"
def is_main_parent(elt):
    relations = elt.get('relations', [])
    for r in relations:
        if r.get('relationTag')=='structure-interne':
            if r.get('resourceId') != elt['id']:
                return False
    return True

def get_ror(elt):
    tmp = [k for k in elt['identifiers'] if k['type']=='ror']
    if tmp:
        for t in tmp:
            if t['active']:
                return tmp[0]['value']
    return None
        
def get_perimeter():
    df = pd.read_json(url_paysage, lines=True)
    data = []
    for e in df.to_dict(orient='records'):
        if is_main_parent(e):
            ror = get_ror(e)
            category = e['category']
            if len(category)==0:
                continue
            cat = category['usualNameFr']
            if cat not in ["Grand établissement relevant d'un autre département ministériel", "Grand établissement", "Communauté d'universités et établissements",
                "Établissement public expérimental",
                'Université', "Organisme de recherche",
                "Institut national polytechnique", "Institut ou école extérieur aux universités",
                "École normale supérieure", "Établissement d'enseignement supérieur privé d'intérêt général",
                "École d'ingénieurs",  "École de commerce et de management", "École nationale supérieure d'ingénieurs",
                "Institut d'étude politique",
                "Centre de lutte contre le cancer", "Institut hospitalo-universitaire", "Centre hospitalier",
                "Établissement privé d'enseignement universitaire", "Établissement d'enseignement supérieur privé rattachés à un EPSCP",
                ]:
                continue
            if 'displayName' not in e:
                continue
            if e['status'] != 'active':
                continue
            name = e['displayName']
            if ror is None:
                continue
            new_e = {'ror':ror, 'nom': name}
            #print(new_e)
            data.append(new_e)
    pd.DataFrame(data).to_csv('/src/config/etablissements_dashboard.csv', sep=';', index=False)
