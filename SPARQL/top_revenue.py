from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?title ?revenue
WHERE {
  GRAPH <http://mydata.utwente.org/movies/boxoffice> {
    ?record a mydata:BoxOfficeRecord ;
            schema1:about ?movie ;
            schema1:Date "2002-12-10"^^xsd:date ;
            schema1:revenue ?revenue .
  }
  ?movie schema1:name ?title .
}
ORDER BY DESC(?revenue)
LIMIT 10
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Movie Title": result["title"]["value"],
            "Revenue (USD)": float(result["revenue"]["value"])
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No Box Office data found for the specified date.")
    else:
        plt.figure(figsize=(12, 8))
        sns.set_style("whitegrid")
        

        ax = sns.barplot(x="Revenue (USD)", y="Movie Title", data=df, palette="rocket")
        
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))

        plt.title("Top 10 Highest Revenue Movies (December 10, 2002)", fontsize=16, weight='bold')
        plt.xlabel("Revenue", fontsize=12)
        plt.ylabel("", fontsize=12)
        
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")