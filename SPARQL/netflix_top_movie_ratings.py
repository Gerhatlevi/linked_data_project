from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX swportal: <http://sw-portal.deri.org/ontologies/swportal#>
PREFIX schema1: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?movieName ?rating
WHERE {
    ?movie a mydata:Movie ;
      schema1:name ?movieName ;
      schema1:ratingValue ?rating .
      
      GRAPH <http://mydata.utwente.org/movies/netflix> {
        ?list a mydata:GlobalTopList ;
        	  schema1:inLanguage "English";
              schema1:Date ?date ;
              ?property ?movie ;
              schema1:category "Films" .
        
        FILTER (?date = "2023-07-23"^^xsd:date)
    } 
} ORDER BY DESC(?rating)
"""

sparql.setQuery(query)
results = sparql.query().convert()
rows = []
for result in results["results"]["bindings"]:
    rows.append({
        "Movie": result["movieName"]["value"],
        "IMDB rating": result["rating"]["value"]
        # "Number of ratings": result["ratingCount"]["value"]
    })

df = pd.DataFrame(rows)
fig, ax = plt.subplots()
ax.axis("off")
# plt.title("Top rated actors/crew with at least 5 movie credits that were in the Dutch Netflix top 10", fontsize=12, weight='bold')


table = pd.plotting.table(ax, df, loc="center", cellLoc="center", colWidths=[0.6, 0.2])
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(1.2, 2.5)
plt.show()
