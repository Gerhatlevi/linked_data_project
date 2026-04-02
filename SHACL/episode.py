from SPARQLWrapper import SPARQLWrapper, JSON

# sparql = SPARQLWrapper("http://localhost:7200/repositories/linked_data_ut")
sparql = SPARQLWrapper("http://localhost:7200/repositories/lined_data_project") # Name on Melle's laptop.
# Yes I made a typo, GraphDB won't let me change the name lol
sparql.setReturnFormat(JSON)

query = """
PREFIX mydata: <http://mydata.utwente.org/movies/>
PREFIX schema1: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?invalidSubject ?property ?value
WHERE {
  ?invalidSubject ?property ?value .
  FILTER (?property IN (schema1:seasonNumber, schema1:episodeNumber))
  
  FILTER NOT EXISTS {
    ?invalidSubject rdf:type mydata:episode .
  }
}
LIMIT 1
"""

sparql.setQuery(query)

try:
    results = sparql.query().convert()
    bindings = results["results"]["bindings"]
    print(results)

    print("--- SHACL First Violation Check ---")
    if not bindings:
        print("SUCCESS: No violations found in the dataset.")
    else:
        violation = bindings[0]
        print("SHACL Violation detected!")
        print(f"Subject:  {violation['invalidSubject']['value']}")
        print(f"Property: {violation['property']['value']}")
        print(f"Value:    {violation['value']['value']}")
        print("\nFix this instance and run the script again to find the next one.")

except Exception as e:
    print(f"Error: {e}")

