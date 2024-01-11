from google.cloud import bigquery
import requests
import json
import datetime

# Project ID Google Cloud
project_id = 'my_project'

# API config
results_per_page = 100
total_pages = 20
base_url = 'https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel@parisdata/records?limit={}&offset={}'

# BigQuery table
destination_table = f'{project_id}.raw_data.bike_avaibility'

def extract_data():
	results = []
	try:
	    for page in range(total_pages):
	        offset = page * results_per_page
	        url = base_url.format(results_per_page, offset)
	        print(f"Extracting page {page + 1}")
	
	        response = requests.get(url, timeout=20)
	        if response.status_code == 200:
	            data = response.json()
	            results.extend(data['results'])
	        else:
	            raise Exception('Request failed with status: {}'.format(response.status_code))
	except requests.exceptions.RequestException as e:
	    raise Exception('An error occured: {}'.format(e))
	
	return results


def format_data(results):

     # Collection datetime (current datetime) 
    created_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    rows_to_insert = [
     (
         row['stationcode'], row['name'], row['is_installed'], row['capacity'],
         row['numdocksavailable'], row['numbikesavailable'], row['mechanical'], row['ebike'],
         row['is_renting'], row['is_returning'], row['duedate'], row['coordonnees_geo']['lon'],
         row['coordonnees_geo']['lat'], row['nom_arrondissement_communes'],
         row['code_insee_commune'], created_at
     )
     for row in results
    ]
    return rows_to_insert

def insert_data_to_bigquery(rows_to_insert):
    client = bigquery.Client(project=project_id)
    table = client.get_table(destination_table)

    # Insert data into BigQuery table
    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        raise Exception('Error while inserting data in BigQuery table: {}'.format(errors))


if __name__ == '__main__':

  data = extract_data()
  rows = format_data(data) 
  insert_data_to_bigquery(rows)
