from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX swportal: <http://sw-portal.deri.org/ontologies/swportal#>
PREFIX schema1: <http://schema.org/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?actorName (COUNT(?list) AS ?weeksInTop10)
WHERE {
    GRAPH <http://mydata.utwente.org/movies/netflix> {
        ?list a mydata:GlobalTopList ;
        	  schema1:inLanguage "English";
              schema1:category "Films" ;
              schema1:Date ?date .
        
        FILTER (YEAR(?date) = 2023)

        VALUES ?pos { 
            swportal:agent_1 swportal:agent_2 swportal:agent_3 swportal:agent_4 swportal:agent_5
            swportal:agent_6 swportal:agent_7 swportal:agent_8 swportal:agent_9 swportal:agent_10 
        }
        ?list ?pos ?movieURI .
    }
    
    ?actor a mydata:Person ;
           schema1:name ?actorName ;
           schema1:workFeatured ?movieURI .
}
GROUP BY ?actorName
ORDER BY DESC(?weeksInTop10)
LIMIT 10
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    data = []
    for result in results["results"]["bindings"]:
        data.append({
            "Actor": result["actorName"]["value"],
            "Weeks in Top 10": int(result["weeksInTop10"]["value"])
        })

    df = pd.DataFrame(data)

    if df.empty:
        print("Cannot find any Netflix records for this category.")
    else:
        plt.figure(figsize=(12, 7))
        sns.set_style("whitegrid")
        
        ax = sns.barplot(x="Weeks in Top 10", y="Actor", data=df, color='darkblue')
        
        plt.title("Actors with Most Presence in Netflix Top 10 (2023)", fontsize=16, weight='bold')
        plt.xlabel("", fontsize=12)
        plt.ylabel("", fontsize=12)
        
        for i, v in enumerate(df["Weeks in Top 10"]):
            ax.text(v + 0.1, i, str(v), color='black', va='center', fontweight='bold')

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"An error occurred while querying: {e}")