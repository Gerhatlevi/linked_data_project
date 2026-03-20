import pandas as pd
from rdflib import OWL, RDFS, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import XSD

df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\filtered_name_basics.tsv', sep='\t', na_values=r'\N')

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA, override=True)

ontology_uri = URIRef("http://mydata.utwente.org/movies/people")
g.add((ontology_uri, RDF.type, OWL.Ontology))

g.add((SCHEMA.Person, RDF.type, OWL.Class))

for prop in [SCHEMA.name, SCHEMA.birthDate, SCHEMA.deathDate]:
    g.add((prop, RDF.type, OWL.DatatypeProperty))
    g.add((prop, RDFS.domain, SCHEMA.Person))

g.add((SCHEMA.workFeatured, RDF.type, OWL.ObjectProperty))
g.add((SCHEMA.workFeatured, RDFS.domain, SCHEMA.Person))

for row in df.itertuples(index=False):
    subject = MYDATA[str(row.nconst)]

    g.add((subject, RDF.type, OWL.NamedIndividual))
    g.add((subject, RDF.type, SCHEMA.Person))
    g.add((subject, SCHEMA.name, Literal(row.primaryName)))
    
    if pd.notna(row.birthYear):
        g.add((subject, SCHEMA.birthDate, Literal(int(row.birthYear), datatype=XSD.gYear)))
        
    if pd.notna(row.deathYear):
        g.add((subject, SCHEMA.deathDate, Literal(int(row.deathYear), datatype=XSD.gYear)))
        
    if pd.notna(row.knownForTitles):
        titles = str(row.knownForTitles).split(',')
        for tconst in titles:
            tconst_clean = tconst.strip()
            if tconst_clean:
                g.add((subject, SCHEMA.workFeatured, MYDATA[tconst_clean]))

g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\imdb_people.ttl', format="turtle")