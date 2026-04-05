import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import OWL, XSD


def triplify():
    df = pd.read_csv('C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\data_files\\used\\global-with-imdb2.csv', low_memory=False)
    df = df.sort_values("weekly_rank")

    # --- PARSE CATEGORY: "Films (English)" -> category="Films", language="English" ---
    def parse_category(cat):
        if "(" in cat:
            base = cat[:cat.index("(")].strip()
            lang = cat[cat.index("(")+1:cat.index(")")].strip()
        else:
            base = cat.strip()
            lang = None
        return base, lang

    # --- PIVOT ---
    pivoted = {}
    for _, row in df.iterrows():
        key = (row["week"], row["category"])
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

    ontology_uri = URIRef("http://mydata.utwente.org/movies/netflix-global")
    g.add((ontology_uri, RDF.type, OWL.Ontology))

    global_list_class = MYDATA.GlobalTopList
    g.add((global_list_class, RDF.type, OWL.Class))

    content_class = MYDATA.Content
    g.add((content_class, RDF.type, OWL.Class))

    movie_class = MYDATA.Movie
    g.add((movie_class, RDF.type, OWL.Class))
    tv_class = MYDATA.TV
    g.add((tv_class, RDF.type, OWL.Class))

    for prop in [SCHEMA.category, SCHEMA.Date, SCHEMA.inLanguage]:
        g.add((prop, RDF.type, OWL.DatatypeProperty))
    for i in range(1, 11):
        g.add((SWPORTAL[f"agent_{i}"], RDF.type, OWL.ObjectProperty))

    # --- TRIPLIFY ---
    for (week, category), tconsts in pivoted.items():
        base_cat, lang = parse_category(category)

        lang_part = f"_{lang}" if lang else ""
        record_id = f"{week}_{base_cat.replace(' ', '_')}{lang_part.replace(' ', '_')}"
        subject = MYDATA[record_id]

        g.add((subject, RDF.type, OWL.NamedIndividual))
        g.add((subject, RDF.type, global_list_class))

        g.add((subject, SCHEMA.Date, Literal(week, datatype=XSD.date)))
        g.add((subject, SCHEMA.category, Literal(base_cat)))
        if lang:
            g.add((subject, SCHEMA.inLanguage, Literal(lang)))

        content_class = tv_class if "tv" in category.lower() else movie_class

        for i, tconst in enumerate(tconsts[:10], start=1):
            obj = MYDATA[str(tconst)]
            g.add((subject, SWPORTAL[f"agent_{i}"], obj))
            g.add((obj, RDF.type, OWL.NamedIndividual))
            g.add((obj, RDF.type, content_class))

    g.serialize(destination='C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\used\\netflix_global.ttl', format='turtle')
    print("Done!")


triplify()