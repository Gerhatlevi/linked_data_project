from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX swportal: <http://sw-portal.deri.org/ontologies/swportal#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?rank ?title
WHERE {
  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?record a mydata:CountryRecord ;
            schema1:Date "2021-07-04"^^xsd:date ;
            schema1:spatialCoverage "Brazil" ;
            schema1:category "TV" ;
            ?property ?movieURI .
            
    VALUES (?property ?rank) {
      (swportal:agent_1 1)
      (swportal:agent_2 2)
      (swportal:agent_3 3)
      (swportal:agent_4 4)
      (swportal:agent_5 5)
    }
  }
  ?movieURI schema1:name ?title .
}
ORDER BY ?rank
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Rank": result["rank"]["value"],
            "TV Show Title": result["title"]["value"]
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No records found for Brazil on this date.")
    else:
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.axis('off')

        plt.title("Top 5 TV Shows in Brazil (July 04, 2021)", fontsize=16, weight='bold', pad=20)

        table = plt.table(cellText=df.values,
                          colLabels=df.columns,
                          cellLoc='left',
                          loc='center',
                          colColours=["#e2e2e2", "#e2e2e2"])

        table.auto_set_font_size(False)
        table.set_fontsize(14)
        table.scale(1.2, 2.5)

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")