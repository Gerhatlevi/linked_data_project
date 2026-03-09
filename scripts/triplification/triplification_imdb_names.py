import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\filtered_name_basics.tsv', sep='\t', na_values=r'\N')

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA, override=True)

for row in df.itertuples(index=False):
    subject = MYDATA[str(row.nconst)]
    
    g.add((subject, RDF.type, SCHEMA.Person))
    g.add((subject, SCHEMA.name, Literal(row.primaryName)))
    
    if pd.notna(row.birthYear):
        g.add((subject, SCHEMA.birthDate, Literal(int(row.birthYear), datatype=XSD.gYear)))
        
    if pd.notna(row.deathYear):
        g.add((subject, SCHEMA.deathDate, Literal(int(row.deathYear), datatype=XSD.gYear)))
        
    if pd.notna(row.knownForTitles):
        titles = str(row.knownForTitles).split(',')
        for tconst in titles:
            g.add((subject, SCHEMA.workFeatured, MYDATA[tconst.strip()]))

g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\imdb_people.ttl', format="turtle")