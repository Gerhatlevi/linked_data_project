import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import OWL, XSD


def triplify():
    df = pd.read_csv("C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\countries-with-imdb.csv", low_memory=False)

    # --- PIVOT: one row per (week, country, category), ranks become agent_1..agent_10 ---
    df = df.sort_values("weekly_rank")
    pivoted = {}

    for _, row in df.iterrows():
        key = (row["week"], row["country_name"], row["country_iso2"], row["category"])
        if key not in pivoted:
            pivoted[key] = []
        pivoted[key].append(row["imdb_tconst"])

    # --- GRAPH SETUP ---
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

    movie_class = MYDATA.Movie
    g.add((movie_class, RDF.type, OWL.Class))
    tv_class = MYDATA.TV
    g.add((tv_class, RDF.type, OWL.Class))

    for prop in [SCHEMA.category, SCHEMA.spatialCoverage, SCHEMA.Date]:
        g.add((prop, RDF.type, OWL.DatatypeProperty))

    for i in range(1, 11):
        g.add((SWPORTAL[f"agent_{i}"], RDF.type, OWL.ObjectProperty))

    # --- TRIPLIFY ---
    for (week, country_name, country_iso2, category), tconsts in pivoted.items():
        record_id = f"{week}_{country_iso2}_{category}"
        subject = MYDATA[record_id]

        g.add((subject, RDF.type, OWL.NamedIndividual))
        g.add((subject, RDF.type, country_record_class))

        g.add((subject, SCHEMA.spatialCoverage, Literal(country_name)))
        g.add((subject, SCHEMA.Date, Literal(week, datatype=XSD.date)))
        g.add((subject, SCHEMA.category, Literal(category)))

        content_class = tv_class if "tv" in category.lower() else movie_class

        for i, tconst in enumerate(tconsts[:10], start=1):
            predicate = SWPORTAL[f"agent_{i}"]
            obj = MYDATA[str(tconst)]
            g.add((subject, predicate, obj))
            g.add((obj, RDF.type, OWL.NamedIndividual))
            g.add((obj, RDF.type, content_class))

    g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\netflix_countries2.ttl', format='turtle')


triplify()