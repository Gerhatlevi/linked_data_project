from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX swportal: <http://sw-portal.deri.org/ontologies/swportal#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?personName (AVG(?rating) AS ?averageRating) (COUNT(?rating) AS ?ratingCount)
WHERE {

  ?person a mydata:Person ;
    schema1:workFeatured ?movie ;
    schema1:name ?personName .
    
  ?movie a mydata:Movie ;
  schema1:ratingValue ?rating .
  
  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?netRecord a mydata:CountryRecord ;
               schema1:spatialCoverage "Brazil" ;
               ?property ?movie .
  }
  
} GROUP BY ?personName
HAVING(?ratingCount > 4.0)
ORDER BY DESC (AVG(?rating))
LIMIT 10
"""

sparql.setQuery(query)
results = sparql.query().convert()
rows = []
for result in results["results"]["bindings"]:
    rows.append({
        "Person": result["personName"]["value"],
        "Average rating": result["averageRating"]["value"]
        # "Number of ratings": result["ratingCount"]["value"]
    })

df = pd.DataFrame(rows)
fig, ax = plt.subplots()
ax.axis("off")
# plt.title("Top rated actors/crew with at least 5 movie credits that were in the Dutch Netflix top 10", fontsize=12, weight='bold')


table = pd.plotting.table(ax, df, loc="center", cellLoc="center", colWidths=[0.3, 0.3])
table.auto_set_font_size(False)
table.set_fontsize(12)
table.scale(1.2, 2.5)
plt.show()
