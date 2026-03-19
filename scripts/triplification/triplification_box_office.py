import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\box_office_with-imdb.csv')

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA)

def clean_num(value):
    if pd.isna(value) or str(value).strip() in ('-', ''):
        return None
    clean_val = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(clean_val)
    except ValueError:
        return None

for row in df.itertuples(index=False):
    record_id = f"boxoffice_{row.imdb_tconst}_{row.date}"
    subject = MYDATA[record_id]

    g.add((subject, SCHEMA.about, MYDATA[str(row.imdb_tconst)]))
    g.add((subject, SCHEMA.position, Literal(int(row.td), datatype=XSD.integer)))
    
    g.add((subject, SCHEMA.Date, Literal(row.date, datatype=XSD.date)))
    
    revenue_val = clean_num(row.todate)
    if revenue_val is not None:
        g.add((subject, SCHEMA.revenue, Literal(revenue_val, datatype=XSD.float)))
        
    theaters_val = clean_num(row.theaters)
    if theaters_val is not None:
        g.add((subject, SCHEMA.interactionCount, Literal(int(theaters_val), datatype=XSD.integer)))

    if pd.notna(row.distributor):
        g.add((subject, SCHEMA.publisher, Literal(row.distributor)))

output_path = 'C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\box_office.ttl'
g.serialize(destination=output_path, format="turtle")