import pandas as pd
from rdflib import Graph, Literal, RDF, Namespace
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_title_basics.tsv', sep='\t', na_values=r'\N')

g = Graph()
schema = Namespace("http://schema.org/")
mydata = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", schema, override=True)
g.bind("mydata", mydata)

for _, row in df.iterrows():
    subject = mydata[row['tconst']]
    
    g.add((subject, schema.additionalType, Literal(row['titleType'])))
    g.add((subject, schema.name, Literal(row['primaryTitle'])))
    
    if pd.notna(row['originalTitle']):
        g.add((subject, schema.alternateName, Literal(row['originalTitle'])))
    
    if pd.notna(row['startYear']):
        g.add((subject, schema.datePublished, Literal(int(row['startYear']), datatype=XSD.gYear)))
        
    if pd.notna(row['runtimeMinutes']):
        g.add((subject, schema.duration, Literal(int(row['runtimeMinutes']), datatype=XSD.integer)))
        
    if pd.notna(row['genres']):
        g.add((subject, schema.genre, Literal(row['genres'])))

g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\imdb_titles.ttl', format='turtle')