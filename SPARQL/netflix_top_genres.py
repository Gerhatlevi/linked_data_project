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

SELECT ?genre (COUNT(?genre) AS ?genreCount)
WHERE {

  ?movie a mydata:Movie ;
  schema1:genre ?genre .

  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?netRecord a mydata:CountryRecord ;
               schema1:spatialCoverage "Italy" ;
               ?property ?movie ;
			   schema1:Date ?date .
	FILTER (YEAR(?date) = 2022)
  }

} GROUP BY ?genre
ORDER BY DESC (COUNT(?genre))
LIMIT 10
"""

sparql.setQuery(query)
results = sparql.query().convert()
rows = []
for result in results["results"]["bindings"]:
    rows.append({
        "Genre": result["genre"]["value"],
        "top 10 occurrences": result["genreCount"]["value"]
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
