from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?name ?birthDate
WHERE {
  BIND(mydata:tt1928130 AS ?movie)
  
  ?person a mydata:Person ;
          schema1:name ?name ;
          schema1:birthDate ?birthDate ;
          schema1:workFeatured ?movie .
          
  FILTER NOT EXISTS { ?person schema1:deathDate ?deathYear }
}
ORDER BY ASC(?birthDate)
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Contributor Name": result["name"]["value"],
            "Birth Year": result["birthDate"]["value"]
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No living contributors found.")
    else:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.axis('off')

        plt.title("Living Contributors: 'Breathe' (tt1928130)", fontsize=16, weight='bold', pad=20)

        table = plt.table(cellText=df.values,
                          colLabels=df.columns,
                          cellLoc='left',
                          loc='center',
                          colColours=["#f2f2f2", "#f2f2f2"])
        
        # 4. Final adjustments for aesthetics
        table.auto_set_font_size(False)
        table.set_fontsize(13)
        table.scale(1.2, 1.6)
        
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")