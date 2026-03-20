import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import OWL, XSD


def triplify():
    df = pd.read_csv('../../data_files/used/countries-with-imdb.csv', low_memory=False)

    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA)

    ontology_uri = URIRef("http://mydata.utwente.org/movies/netflix-countries")
    g.add((ontology_uri, RDF.type, OWL.Ontology))

    country_record_class = MYDATA.CountryRecord
    g.add((country_record_class, RDF.type, OWL.Class))

    data_props = [SCHEMA.spatialCoverage, SCHEMA.contentLocation, SCHEMA.Date, 
                  SCHEMA.category, SCHEMA.position, SCHEMA.name, SCHEMA.alternateName]
    for prop in data_props:
        g.add((prop, RDF.type, OWL.DatatypeProperty))

    g.add((SCHEMA.about, RDF.type, OWL.ObjectProperty))

    for row in df.itertuples(index=False):
        record_id = f"netflix_countries_{row.country_iso2}_{row.imdb_tconst}_{row.week}"
        subject = MYDATA[record_id]

        g.add((subject, RDF.type, OWL.NamedIndividual))
        g.add((subject, RDF.type, country_record_class))

        g.add((subject, SCHEMA.about, MYDATA[str(row.imdb_tconst)]))

        g.add((subject, SCHEMA.spatialCoverage, Literal(row.country_name)))
        g.add((subject, SCHEMA.contentLocation, Literal(row.country_iso2)))

        g.add((subject, SCHEMA.Date, Literal(row.week, datatype=XSD.date)))
        g.add((subject, SCHEMA.category, Literal(row.category)))

        g.add((subject, SCHEMA.position, Literal(int(row.weekly_rank), datatype=XSD.integer)))

        g.add((subject, SCHEMA.name, Literal(row.show_title)))

        if pd.notna(row.season_title):
            g.add((subject, SCHEMA.alternateName, Literal(row.season_title)))


    output_path = '../../RDFs/used/netflix_countries.ttl'
    g.serialize(destination=output_path, format="turtle")

def triplifiy_reformatted():
    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    SWPORTAL = Namespace("http://sw-portal.deri.org/ontologies/swportal#")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA, override=True)
    g.bind("swportal", SWPORTAL, override=True)

    ontology_uri = URIRef("http://mydata.utwente.org/movies/netflix-countries")
    g.add((ontology_uri, RDF.type, OWL.Ontology))

    country_record_class = MYDATA.CountryRecord
    g.add((country_record_class, RDF.type, OWL.Class))

    for prop in [SCHEMA.category, SCHEMA.spatialCoverage, SCHEMA.Date]:
        g.add((prop, RDF.type, OWL.DatatypeProperty))

    for i in range(1, 11):
        g.add((SWPORTAL[f"agent_{i}"], RDF.type, OWL.ObjectProperty))

    df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\countries_reformatted.csv', index_col=0)
    for index, row in df.iterrows():
        record_id = index
        subject = MYDATA[record_id]

        g.add((subject, RDF.type, OWL.NamedIndividual))
        g.add((subject, RDF.type, country_record_class))

        g.add((subject, SCHEMA.category, Literal(row.category)))
        g.add((subject, SCHEMA.spatialCoverage, Literal(row.country)))
        g.add((subject, SCHEMA.Date, Literal(row.week, datatype=XSD.date)))

        for i in range(1, 11):
            rank_col = f'at_rank{i}'
            movie_id = str(row[rank_col])
            
            if pd.notna(row[rank_col]) and movie_id != "NaN":
                predicate = SWPORTAL[f"agent_{i}"]
                object_uri = MYDATA[movie_id] 
                g.add((subject, predicate, object_uri))

    g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\netflix_countries_reformatted.ttl', format='turtle')

triplifiy_reformatted()
