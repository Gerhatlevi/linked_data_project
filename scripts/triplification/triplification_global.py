import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD

def triplify():
    df = pd.read_csv('../../data_files/used/global-with-imdb.csv', low_memory=False)

    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA, override=True)

    for row in df.itertuples(index=False):
        record_id = f"global_{row.imdb_tconst}_{row.week}"
        subject = MYDATA[record_id]

        g.add((subject, SCHEMA.category, Literal(row.category)))
        g.add((subject, SCHEMA.position, Literal(int(row.weekly_rank), datatype=XSD.integer)))
        g.add((subject, SCHEMA.name, Literal(row.show_title)))
        g.add((subject, SCHEMA.alternateName, Literal(row.season_title)))
        g.add((subject, SCHEMA.duration, Literal(row.runtime, datatype=XSD.float)))

        if pd.notna(row.weekly_views):
            g.add((subject, SCHEMA.interactionCount, Literal(int(row.weekly_views), datatype=XSD.integer)))

        g.add((subject, SCHEMA.about, Literal(row.imdb_tconst)))
    g.serialize(
        destination='../../RDFs/used/netflix_global.ttl',
        format="turtle")

# Alternative triplifcation for the reformatted data:
def triplifiy_reformatted():
    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    SWPORTAL = Namespace("http://sw-portal.deri.org/ontologies/swportal#")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA, override=True)

    df = pd.read_csv('../../data_files/used/global_reformatted.csv', index_col=0)
    for index, row in df.iterrows():
        record_id = index
        subject = MYDATA[record_id]

        g.add((subject, SCHEMA.category, Literal(row.category)))
        g.add((subject, SCHEMA.inLanguage, Literal(row.language)))
        g.add((subject, SCHEMA.Date, Literal(row.week, datatype=XSD.date)))

        for i in range(1, 11):
            col_name = f'at_rank{i}'
            movie_id = str(row[col_name])
            
            if pd.notna(row[col_name]) and movie_id != "nan" and movie_id != "NaN":
                predicate = SWPORTAL[f"agent_{i}"]
                object_uri = MYDATA[movie_id]
                g.add((subject, predicate, object_uri))

    g.serialize(destination='../../RDFs/used/netflix_global_reformatted.ttl', format='turtle')


triplifiy_reformatted()
