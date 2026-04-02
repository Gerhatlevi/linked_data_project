import pyshacl
import pathlib
from rdflib import Graph

# base_path = "../RDFs/OWL/"
# files = ["netflix_countries.ttl", "netflix_global.ttl"]
g = Graph()
g.parse('../RDFs/OWL/imdb_episodes.ttl')
# full_files = [g.parse(pathlib.Path(file + base_path)) for file in files]

pyshacl.validate(g,shacl_graph='example.ttl')




