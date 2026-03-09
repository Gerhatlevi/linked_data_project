import pandas as pd
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\filtered_title_ratings.tsv', sep='\t', na_values=r'\N')

g = Graph()

SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

# 2. Triplifikáció
for row in df.itertuples(index=False):
    subject = MYDATA[str(row.tconst)]
    
    if pd.notna(row.averageRating):
        g.add((subject, SCHEMA.ratingValue, Literal(float(row.averageRating), datatype=XSD.float)))
    
    if pd.notna(row.numVotes):
        g.add((subject, SCHEMA.ratingCount, Literal(int(row.numVotes), datatype=XSD.integer)))

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA, override=True)


g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\imdb_movie_ratings.ttl', format="turtle")