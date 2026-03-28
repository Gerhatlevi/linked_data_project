from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX swportal: <http://sw-portal.deri.org/ontologies/swportal#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?personName
WHERE {
  ?movie a mydata:Movie .
  
  ?person a mydata:Person ;
          schema1:name ?personName ;
          schema1:workFeatured ?movie .

  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?netRecord a mydata:CountryRecord ;
               schema1:spatialCoverage "Netherlands" ;
               ?property ?movie .
    
    FILTER (?property IN (swportal:agent_1, swportal:agent_2, swportal:agent_3))
  }

  GRAPH <http://mydata.utwente.org/movies/boxoffice> {
    ?boRecord schema1:about ?movie ;
              schema1:revenue ?revenue .
    
    FILTER(?revenue > 1000000)
  }
}
ORDER BY ?personName
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    names = [result["personName"]["value"] for result in results["results"]["bindings"]]

    if not names:
        print("No matching records found.")
    else:
        num_cols = 3
        rows_per_col = int(np.ceil(len(names) / num_cols))

        padded_names = names + [""] * (rows_per_col * num_cols - len(names))
        
        reshaped_data = np.array(padded_names).reshape(rows_per_col, num_cols, order='F')
        df = pd.DataFrame(reshaped_data, columns=[f"Column {i+1}" for i in range(num_cols)])

        fig_height = max(5, len(df) * 0.4 + 2)
        fig, ax = plt.subplots(figsize=(14, fig_height))
        ax.axis('off')

        ax.text(0.5, 0.98, "Contributors: Top 3 NL & High Revenue", 
                horizontalalignment='center', 
                fontsize=18, 
                weight='bold', 
                transform=ax.transAxes)

        table = plt.table(cellText=df.values,
                          colLabels=None,
                          cellLoc='left',
                          loc='center',
                          cellColours=[["#f8f9fa" if i % 2 == 0 else "#ffffff"] * num_cols for i in range(len(df))])

        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.6)

        plt.tight_layout()
        plt.show()

except Exception as e:
    print(f"Error: {e}")