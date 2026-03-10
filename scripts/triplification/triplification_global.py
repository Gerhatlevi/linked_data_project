import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF
from rdflib.namespace import XSD

def triplify():
    df = pd.read_csv('../../data_files/global-with-imdb.csv', low_memory=False)

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
        destination='../../RDFs/netflix_global.ttl',
        format="turtle")

# Alternative triplifcation for the reformatted data:
def triplifiy_reformatted():
    g = Graph()
    SCHEMA = Namespace("http://schema.org/")
    SWPORTAL = Namespace("http://sw-portal.deri.org/ontologies/swportal#")
    MYDATA = Namespace("http://mydata.utwente.org/movies/")

    g.bind("schema", SCHEMA, override=True)
    g.bind("mydata", MYDATA, override=True)

    df = pd.read_csv('../../data_files/global_reformatted.csv', index_col=0)
    for index, row in df.iterrows():
        record_id = index
        subject = MYDATA[record_id]

        g.add((subject, SCHEMA.category, Literal(row.category)))
        g.add((subject, SCHEMA.inLanguage, Literal(row.language)))
        g.add((subject, SCHEMA.Date, Literal(row.week, datatype=XSD.date)))

        g.add((subject, SWPORTAL.agent_1, Literal(row.at_rank1)))
        g.add((subject, SWPORTAL.agent_2, Literal(row.at_rank2)))
        g.add((subject, SWPORTAL.agent_3, Literal(row.at_rank3)))
        g.add((subject, SWPORTAL.agent_4, Literal(row.at_rank4)))
        g.add((subject, SWPORTAL.agent_5, Literal(row.at_rank5)))
        g.add((subject, SWPORTAL.agent_6, Literal(row.at_rank6)))
        g.add((subject, SWPORTAL.agent_7, Literal(row.at_rank7)))
        g.add((subject, SWPORTAL.agent_8, Literal(row.at_rank8)))
        g.add((subject, SWPORTAL.agent_9, Literal(row.at_rank9)))
        g.add((subject, SWPORTAL.agent_10, Literal(row.at_rank10)))

    g.serialize(destination='../../RDFs/netflix_global_reformatted.ttl', format='turtle')


triplifiy_reformatted()
