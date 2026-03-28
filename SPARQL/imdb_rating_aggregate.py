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

  # Tartományok meghatározása (Binning)
  BIND(
    IF(?rating < 3, "0-3 (Horrible)",
      IF(?rating < 5, "3-5 (Poor)",
        IF(?rating < 8, "5-8 (Average/Good)", "8-10 (Excellent)")
      )
    ) AS ?ratingRange
  )
}
GROUP BY ?ratingRange
ORDER BY ?ratingRange
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

    plt.figure(figsize=(10, 6))
    sns.set_style("white")
    
    order = ["0-3 (Horrible)", "3-5 (Poor)", "5-8 (Average/Good)", "8-10 (Excellent)"]
    
    ax = sns.barplot(x="Range", y="Count", data=df, order=order, palette="coolwarm")

    for p in ax.patches:
        ax.annotate(f'{int(p.get_height())}', 
                   (p.get_x() + p.get_width() / 2., p.get_height()), 
                   ha = 'center', va = 'center', 
                   xytext = (0, 9), 
                   textcoords = 'offset points')

    plt.title("Short Films Rating Distribution (At Least 100 Votes)", fontsize=14)
    plt.xlabel("Rating Range", fontsize=12)
    plt.ylabel("Number of Films", fontsize=12)
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"Error: {e}")