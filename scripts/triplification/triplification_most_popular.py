import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD
df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\most-popular-with-imdb.csv', low_memory=False)

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA)

for row in df.itertuples(index=False):
    record_id = f"most_popular_{row.imdb_tconst}"
    subject = MYDATA[record_id]

    g.add((subject, SCHEMA.about, MYDATA[str(row.imdb_tconst)]))

    g.add((subject, SCHEMA.category, Literal(row.category)))
    g.add((subject, SCHEMA.position, Literal(int(row.rank), datatype=XSD.integer)))
    g.add((subject, SCHEMA.name, Literal(row.show_title)))
    
    if pd.notna(row.season_title):
        g.add((subject, SCHEMA.alternateName, Literal(row.season_title)))
    
    if pd.notna(row.views_first_91_days):
        g.add((subject, SCHEMA.interactionCount, Literal(int(row.views_first_91_days), datatype=XSD.integer)))

output_path = 'C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\netflix_most-popular.ttl'
g.serialize(destination=output_path, format="turtle")