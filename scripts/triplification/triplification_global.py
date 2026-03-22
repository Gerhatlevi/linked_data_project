import pandas as pd
from rdflib import OWL, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import XSD

# Alternative triplifcation for the reformatted data:
def triplifiy_reformatted():
    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    SWPORTAL = Namespace("http://sw-portal.deri.org/ontologies/swportal#")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA, override=True)
    g.bind("swportal", SWPORTAL, override=True)

    ontology_uri = URIRef("http://mydata.utwente.org/movies/netflix-global")
    g.add((ontology_uri, RDF.type, OWL.Ontology))

    global_toplist_class = MYDATA.GlobalTopList
    g.add((global_toplist_class, RDF.type, OWL.Class))

    content_class = MYDATA.Content
    g.add((content_class, RDF.type, OWL.Class))

    content_class = MYDATA.Content
    g.add((content_class, RDF.type, OWL.Class))
    movie_class = MYDATA.Movie
    g.add((movie_class, RDF.type, OWL.Class))
    TV_class = MYDATA.TV
    g.add((TV_class, RDF.type, OWL.Class))

    for prop in [SCHEMA.category, SCHEMA.inLanguage, SCHEMA.Date]:
        g.add((prop, RDF.type, OWL.DatatypeProperty))

    for i in range(1, 11):
        g.add((SWPORTAL[f"agent_{i}"], RDF.type, OWL.ObjectProperty))

    df = pd.read_csv('../../data_files/used/global_reformatted.csv', index_col=0)
    for index, row in df.iterrows():
        record_id = index
        subject = MYDATA[record_id]

        g.add((subject, RDF.type, OWL.NamedIndividual))
        g.add((subject, RDF.type, global_toplist_class))

        g.add((subject, SCHEMA.category, Literal(row.category)))
        g.add((subject, SCHEMA.inLanguage, Literal(row.language)))
        g.add((subject, SCHEMA.Date, Literal(row.week, datatype=XSD.date)))

        for i in range(1, 11):
            col_name = f'at_rank{i}'
            movie_id = str(row[col_name])
            g.add((MYDATA[movie_id], RDF.type, content_class))
            g.add((MYDATA[movie_id], RDF.type, OWL.NamedIndividual))
            if pd.notna(row[col_name]) and movie_id != "nan" and movie_id != "NaN":
                predicate = SWPORTAL[f"agent_{i}"]
                object_uri = MYDATA[movie_id]
                g.add((subject, predicate, object_uri))
                g.add((object_uri, RDF.type, content_class))
                if row.category == 'TV':
                    g.add((object_uri, RDF.type, TV_class))
                else:
                    g.add((object_uri, RDF.type, movie_class))



    g.serialize(destination='../../RDFs/used/netflix_global_reformatted.ttl', format='turtle')


triplifiy_reformatted()
