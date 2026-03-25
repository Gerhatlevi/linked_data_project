import os

input_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\OWL\\imdb_titles.ttl"
output_path = "C:\\Users\\leven\\Erasmus\\3_quartile\\LDSW\\Project\\linked_data_project\\RDFs\\OWL\\imdb_titles_cleaned.ttl"

keyword = "schema1:additionalType"
allowed_value = '"short"'

with open(input_path, 'r', encoding='utf-8') as f_in, \
     open(output_path, 'w', encoding='utf-8') as f_out:
    
    for line in f_in:
        if keyword in line:
            if allowed_value not in line:
                continue
        
        f_out.write(line)