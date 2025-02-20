import requests
import pandas as pd
import csv
import os

# API endpoints
applications_url = "https://rpa.casthighlight.com/WS2/domains/14524/applications"
results_url = "https://rpa.casthighlight.com/WS2/domains/14524/applications/{}/results"
export_url = "https://rpa.casthighlight.com/WS/campaigns/csv/export/all?companySwitch={}&applicationResultId={}"

# Replace these with your actual credentials for Basic Authentication
username = 'j.kumar2+sandbox@castsoftware.com'  # Replace with your username
password = 'Welcome@2025'  # Replace with your password

# Read the CSV file containing application names
csv_file = 'applications.csv'  # Update with your CSV file name
output_csv = 'applications_with_results_ids.csv'  # This will store the application names and their results IDs
output_parent_folder = 'exported_csvs'  # This will store the exported CSV zip files in a parent folder

# Load the CSV into a pandas dataframe
df = pd.read_csv(csv_file)

# Ensure the output folder exists
os.makedirs(output_parent_folder, exist_ok=True)

# Step 1: Fetch all applications for the domain
try:
    response = requests.get(applications_url, auth=(username, password))
    response.raise_for_status()  # Raise an error for bad status codes

    # Assuming the response contains application data in a JSON format
    all_applications = response.json()

    # Step 2: Build a dictionary for application names and their corresponding IDs
    application_dict = {}
    for app in all_applications:
        application_name = app.get('name', '').lower()  # Ensure case-insensitivity
        application_id = app.get('id')
        if application_name and application_id:
            application_dict[application_name] = application_id

except requests.exceptions.RequestException as e:
    print(f"Error fetching data from the API: {e}")
    exit(1)

# Step 3: Fetch results ID for each application and add to CSV
results = []

# Iterate over each application name in the CSV file
for index, row in df.iterrows():
    application_name = row['application_name'].lower()  # Ensure case-insensitivity
    
    # Match the application name with the fetched list
    application_id = application_dict.get(application_name, None)
    
    # If we found the application_id, fetch its results
    result_id = None
    if application_id:
        try:
            # Call the second API to get the results for this application
            results_response = requests.get(results_url.format(application_id), auth=(username, password))
            results_response.raise_for_status()  # Raise an error for bad status codes
            
            # Assuming the results data is in a JSON format
            results_data = results_response.json()
            
            # Extract the results ID (adjust based on actual API response structure)
            if isinstance(results_data, list) and len(results_data) > 0:
                result_id = results_data[0].get('id', None)  # Adjust based on the actual key for result_id
        except requests.exceptions.RequestException as e:
            print(f"Error fetching results for application {application_name}: {e}")
    
    # Add the result to the list
    results.append({
        'application_name': row['application_name'],  # Keep the original name
        'application_id': application_id,
        'result_id': result_id
    })

# Write the results to a new CSV file
with open(output_csv, mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=['application_name', 'application_id', 'result_id'])
    writer.writeheader()
    writer.writerows(results)

print(f"Results have been saved to {output_csv}")

# Step 4: Download the CSV.zip for each application
for row in results:
    application_name = row['application_name']
    application_id = row['application_id']
    result_id = row['result_id']

    if pd.notna(application_id) and pd.notna(result_id):
        try:
            # Create a folder for each application
            application_folder = os.path.join(output_parent_folder, application_name)
            os.makedirs(application_folder, exist_ok=True)

            # Construct the export URL using application_id and result_id
            export_url_with_params = export_url.format(application_id, result_id)

            # Request the CSV.zip file
            export_response = requests.get(export_url_with_params, auth=(username, password))
            export_response.raise_for_status()  # Raise an error for bad status codes

            # Save the CSV.zip file to the application folder
            zip_file_path = os.path.join(application_folder, f"csvs.zip")
            with open(zip_file_path, 'wb') as f:
                f.write(export_response.content)

            print(f"CSV.zip for {application_name} saved to {zip_file_path}")

        except requests.exceptions.RequestException as e:
            print(f"Error exporting CSV for {application_name} (ID: {application_id}, Result ID: {result_id}): {e}")

print("Export process completed.")
