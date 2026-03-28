from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>

SELECT ?publisher (SUM(?maxRevenue) AS ?totalRevenue)
WHERE {
  {
    SELECT ?movie (MAX(?revenue) AS ?maxRevenue) ?publisher
    WHERE {
      GRAPH <http://mydata.utwente.org/movies/boxoffice> {
        ?record schema1:about ?movie ;
                schema1:publisher ?publisher ;
                schema1:revenue ?revenue .
      }
    }
    GROUP BY ?movie ?publisher
  }
}
GROUP BY ?publisher
ORDER BY DESC(?totalRevenue)
LIMIT 10
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    data = []
    for result in results["results"]["bindings"]:
        data.append({
            "Publisher": result["publisher"]["value"],
            "Total Revenue": float(result["totalRevenue"]["value"])
        })

    df = pd.DataFrame(data)

    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    
    ax = sns.barplot(x="Total Revenue", y="Publisher", data=df, palette="viridis")
    
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.0f}M'))
    
    plt.title("Top 10 Movie Studios", fontsize=16, weight='bold')
    plt.xlabel("Revenue")
    plt.ylabel("")
    
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Error: {e}")