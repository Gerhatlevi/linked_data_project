from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>

SELECT ?ratingRange (COUNT(?movieURI) AS ?movieCount)
WHERE {
  ?movieURI a mydata:Movie ;
            schema1:additionalType "short" ;
            schema1:ratingValue ?rating ;
            schema1:ratingCount ?votes .
  
  FILTER (?votes >= 100)

  BIND(
    IF(?rating < 2, "1-2 (Very Poor)",
      IF(?rating < 4, "2-4 (Poor)",
        IF(?rating < 6, "4-6 (Average)",
          IF(?rating < 8, "6-8 (Good)", "8-10 (Excellent)")
        )
      )
    ) AS ?ratingRange
  )
}
GROUP BY ?ratingRange
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Range": result["ratingRange"]["value"],
            "Count": int(result["movieCount"]["value"])
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No data found for the given filters.")
    else:
        plt.figure(figsize=(12, 6))
        sns.set_style("whitegrid")
        
        order = ["1-2 (Very Poor)", "2-4 (Poor)", "4-6 (Average)", "6-8 (Good)", "8-10 (Excellent)"]
        
        ax = sns.barplot(x="Range", y="Count", data=df, order=order, palette="coolwarm")

        for p in ax.patches:
            ax.annotate(f'{int(p.get_height())}', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha='center', va='center', 
                       xytext=(0, 9), 
                       textcoords='offset points',
                       fontsize=11, weight='bold')

        plt.title("Short Films Rating Distribution", fontsize=15, weight='bold')
        plt.xlabel("", fontsize=12)
        plt.ylabel("Number of Films", fontsize=12)
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")