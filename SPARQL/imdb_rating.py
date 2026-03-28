from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>

SELECT ?title ?rating
WHERE {
  ?movieURI a mydata:Movie ;
            schema1:name ?title ;
            schema1:additionalType "short" ;
            schema1:ratingValue ?rating ;
            schema1:ratingCount ?ratingCount .
  
  FILTER (?rating > 7.5 && ?ratingCount > 100)
}
ORDER BY DESC(?rating)
LIMIT 15
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Title": result["title"]["value"],
            "Rating": float(result["rating"]["value"])
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No results found with the specified criteria.")
    else:
        plt.figure(figsize=(10, 6))
        sns.set_style("whitegrid")
        
        ax = sns.barplot(x="Rating", y="Title", data=df, palette="viridis")
        
        for i in ax.containers:
            ax.bar_label(i, padding=3)

        plt.title("Top Rated Short Films", fontsize=15)
        plt.xlabel("")
        plt.ylabel("")
        plt.xlim(7.0, 10.0)
        
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error occurred during query execution: {e}")