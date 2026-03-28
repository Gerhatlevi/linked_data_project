from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>

SELECT ?seasonNumber (COUNT(?episode) AS ?episodeCount)
WHERE {
  ?episode schema1:isPartOf mydata:tt2741602 ;
           schema1:seasonNumber ?seasonNumber .
}
GROUP BY ?seasonNumber
ORDER BY ASC(?seasonNumber)
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    rows = []
    for result in results["results"]["bindings"]:
        rows.append({
            "Season": int(result["seasonNumber"]["value"]),
            "Episodes": int(result["episodeCount"]["value"])
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("No data found for this series ID.")
    else:
        plt.figure(figsize=(10, 6))
        sns.set_style("whitegrid")
        
        ax = sns.barplot(x="Season", y="Episodes", data=df, color="#4e79a7")
        
        for p in ax.patches:
            ax.annotate(f'{int(p.get_height())}', 
                       (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha='center', va='center', 
                       xytext=(0, 9), 
                       textcoords='offset points',
                       fontsize=12, weight='bold')

        plt.title("Number of Episodes per Season for 'The Blacklist'", fontsize=14)
        plt.xlabel("Season Number", fontsize=12)
        plt.ylabel("", fontsize=12)
        plt.ylim(0, df["Episodes"].max() + 5)
        
        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")