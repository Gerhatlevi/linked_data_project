import pandas as pd
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\filtered_title_episodes.tsv', sep='\t', na_values=r'\N')

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

for row in df.itertuples(index=False):
    subject = MYDATA[str(row.tconst)]
    
    if pd.notna(row.parentTconst):
        g.add((subject, SCHEMA.isPartOf, MYDATA[str(row.parentTconst)]))
    
    if pd.notna(row.seasonNumber):
        g.add((subject, SCHEMA.seasonNumber, Literal(int(row.seasonNumber), datatype=XSD.integer)))
        
    if pd.notna(row.episodeNumber):
        g.add((subject, SCHEMA.episodeNumber, Literal(int(row.episodeNumber), datatype=XSD.integer)))

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA, override=True)

g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\imdb_episodes.ttl', format="turtle")