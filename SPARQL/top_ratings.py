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

SELECT ?country (AVG(?rating) AS ?averageRating)
WHERE {
  ?movie a mydata:Movie ;
  schema1:ratingValue ?rating ;
  schema1:name ?movieName .

  GRAPH <http://mydata.utwente.org/movies/netflix> {
    ?netRecord a mydata:CountryRecord ;
               schema1:spatialCoverage ?country ;
               ?property ?movie .
  }
  
} GROUP BY ?country
ORDER BY DESC (AVG(?rating))

"""

sparql.setQuery(query)
results = sparql.query().convert()
print(results)