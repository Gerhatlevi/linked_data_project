import pandas as pd
from rdflib import Graph, Literal, RDF, Namespace, OWL, URIRef
from rdflib.namespace import XSD

df = pd.read_csv('../../data_files/used/filtered_title_basics.tsv', sep='\t', na_values=r'\N')

g = Graph()
schema = Namespace("http://schema.org/")
MYDATA = Namespace("http://mydata.utwente.org/movies/")
owl = Namespace("http://www.w3.org/2002/07/owl#")

g.bind("schema", schema, override=True)
g.bind("mydata", MYDATA)

ontology_uri = URIRef("http://mydata.utwente.org/movies/")
g.add((ontology_uri, RDF.type, OWL.Ontology))

content_class = MYDATA.Content
g.add((content_class, RDF.type, OWL.Class))
movie_class = MYDATA.Movie
g.add((movie_class, RDF.type, OWL.Class))
TV_class = MYDATA.TV
g.add((TV_class, RDF.type, OWL.Class))
season_class = MYDATA.season
g.add((season_class, RDF.type, OWL.Class))
episode_class = MYDATA.episode
g.add((episode_class, RDF.type, OWL.Class))


properties = [
    schema.additionalType, schema.name, schema.alternateName, 
    schema.datePublished, schema.duration, schema.genre
]
for prop in properties:
    g.add((prop, RDF.type, OWL.DatatypeProperty))

for _, row in df.iterrows():
    subject = MYDATA[row['tconst']]

    g.add((subject, RDF.type, OWL.NamedIndividual))
    # Add the correct class of content
    g.add((subject, RDF.type, content_class))
    category = row['titleType']
    match category:
        case 'short' | 'movie' | 'tvShort' | 'tvMovie':
            g.add((subject, RDF.type, movie_class))
        case 'tvSpecial':
            g.add((subject, RDF.type, TV_class))
        case 'tvSeries' | 'tvMiniSeries':
            g.add((subject, RDF.type, TV_class))
            g.add((subject, RDF.type, season_class))
        case 'tvEpisode':
            g.add((subject, RDF.type, TV_class))
            g.add((subject, RDF.type, episode_class))

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

g.serialize(destination='../../RDFs/used/imdb_titles.ttl', format='turtle')