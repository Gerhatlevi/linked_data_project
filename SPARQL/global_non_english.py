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

SELECT ?date ?title
WHERE {
  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?list a mydata:GlobalTopList ;
          schema1:category "Films" ;
          schema1:inLanguage "Non-English" ;
          schema1:Date ?date ;
          swportal:agent_1 ?movieURI .
  }
  ?movieURI schema1:name ?title .
}
ORDER BY DESC(?date)
LIMIT 10
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Date": result["date"]["value"],
            "Top 1 Non-English Film": result["title"]["value"]
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No Netflix records found for this category.")
    else:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.axis('off')

        plt.title("Weekly #1 Films on Netflix", fontsize=16, weight='bold', pad=20)

        the_table = plt.table(cellText=df.values,
                              colLabels=df.columns,
                              cellLoc='center',
                              loc='center',
                              colColours=["#d1e7dd", "#d1e7dd"])

        the_table.auto_set_font_size(False)
        the_table.set_fontsize(12)
        the_table.scale(1.2, 2.5)

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")