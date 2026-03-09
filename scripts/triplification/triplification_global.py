import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\global-with-imdb.csv', low_memory=False)

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA, override=True)

for row in df.itertuples(index=False):
    record_id = f"global_{row.imdb_tconst}_{row.week}"
    subject = MYDATA[record_id]

    g.add((subject, SCHEMA.category, Literal(row.category)))
    g.add((subject, SCHEMA.position, Literal(int(row.weekly_rank), datatype=XSD.integer)))
    g.add((subject, SCHEMA.name, Literal(row.show_title)))
    g.add((subject, SCHEMA.alternateName, Literal(row.season_title)))
    g.add((subject, SCHEMA.duration, Literal(row.runtime, datatype=XSD.float)))
    
    if pd.notna(row.weekly_views):
        g.add((subject, SCHEMA.interactionCount, Literal(int(row.weekly_views), datatype=XSD.integer)))

    g.add((subject, SCHEMA.about, Literal(row.imdb_tconst)))

g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\netflix_global.ttl', format="turtle")