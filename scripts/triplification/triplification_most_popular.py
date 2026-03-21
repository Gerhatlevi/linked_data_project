import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, OWL, URIRef
from rdflib.namespace import  XSD
df = pd.read_csv('../../data_files/used/most_popular_with_imdb.csv', low_memory=False)

g = Graph()
SCHEMA = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")

g.bind("schema", SCHEMA, override=True)
g.bind("mydata", MYDATA)

ontology_uri = URIRef("http://mydata.utwente.org/movies/netflix-most-popular")
g.add((ontology_uri, RDF.type, OWL.Ontology))

popularity_class = MYDATA.PopularityRecord
g.add((popularity_class, RDF.type, OWL.Class))

data_props = [SCHEMA.category, SCHEMA.position, SCHEMA.name, SCHEMA.alternateName, SCHEMA.interactionCount, SCHEMA.inLanguage]
for prop in data_props:
    g.add((prop, RDF.type, OWL.DatatypeProperty))

g.add((SCHEMA.about, RDF.type, OWL.ObjectProperty))

for row in df.itertuples(index=False):
    record_id = f"most_popular_{row.imdb_tconst}"
    subject = MYDATA[record_id]

    g.add((subject, RDF.type, OWL.NamedIndividual))
    g.add((subject, RDF.type, popularity_class))

    g.add((subject, SCHEMA.about, MYDATA[str(row.imdb_tconst)]))

    g.add((subject, SCHEMA.category, Literal(row.category)))
    g.add((subject, SCHEMA.position, Literal(int(row.rank), datatype=XSD.integer)))
    g.add((subject, SCHEMA.name, Literal(row.show_title)))
    g.add((subject, SCHEMA.inLanguage, Literal(row.language)))
    
    if pd.notna(row.season_title):
        g.add((subject, SCHEMA.alternateName, Literal(row.season_title)))
    
    if pd.notna(row.views_first_91_days):
        val = int(row.views_first_91_days)
        g.add((subject, SCHEMA.interactionCount, Literal(val, datatype=XSD.integer)))

output_path = '../../RDFs/used/netflix_most_popular.ttl'
g.serialize(destination=output_path, format="turtle")