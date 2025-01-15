import pandas as pd
import requests

# Constants for the API
GEOCODING_API_URL = "https://maps.googleapis.com/maps/api/geocode/json"
API_KEY = "AIzaSyAEFLrb9EzPRuPvz0s_ITTbvHv3Pt1QAnU"  # Replace with your actual API key

def process_csv(file_path, output_path, required_columns):
    """
    Processes a CSV file to remove columns with all NaN values and retain specific columns.
    """
    data = pd.read_csv(file_path)
    data = data.dropna(axis=1, how='all')
    columns_to_keep = [col for col in required_columns if col in data.columns]
    processed_data = data[columns_to_keep]
    processed_data.to_csv(output_path, index=False)
    print(f"Processed file saved to: {output_path}")
    return processed_data

def split_dataset(input_file, output_file1, output_file2):
    """
    Splits a dataset into two equal parts and saves them as separate files.
    """
    data = pd.read_csv(input_file)
    midpoint = len(data) // 2
    data.iloc[:midpoint].to_csv(output_file1, index=False)
    data.iloc[midpoint:].to_csv(output_file2, index=False)
    print(f"Dataset split into: {output_file1} and {output_file2}")

def concatenate_address_components(dataset, address_columns):
    """
    Concatenates address components into a single column.
    """
    def concatenate_address(row):
        address_parts = [str(row[col]) for col in address_columns if pd.notnull(row.get(col, None))]
        return ', '.join(address_parts)

    dataset['FullAddress'] = dataset.apply(concatenate_address, axis=1)
    return dataset

def geocode_address(address):
    """
    Fetches geocoding data for a given address using the Google Maps API.
    """
    params = {"address": address, "key": API_KEY}
    response = requests.get(GEOCODING_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    print(f"Error: {response.status_code} for address: {address}")
    return None

def extract_geocoding_info(geocode_data, building_components, street_components, exclude_components):
    """
    Extracts building and street information from geocoding data.
    """
    building_info, street_info = [], []
    if geocode_data and "results" in geocode_data and geocode_data["results"]:
        for component in geocode_data["results"][0]["address_components"]:
            types = set(component["types"])
            if types.intersection(building_components) and not types.intersection(exclude_components):
                building_info.append(component["long_name"])
            elif types.intersection(street_components):
                street_info.append(component["long_name"])
    return building_info, street_info

def process_addresses(dataset, address_column):
    """
    Processes addresses using the Google Maps API and extracts building and street information.
    """
    building_components = {"subpremise", "sublocality", "locality"}
    street_components = {"street_number", "route", "street_name"}
    exclude_components = {"locality", "administrative_area_level_1", "country"}

    building_info_list, street_info_list = [], []

    for _, row in dataset.iterrows():
        address = row[address_column]
        geocode_data = geocode_address(address)
        if geocode_data and geocode_data.get("status") == "OK":
            building_info, street_info = extract_geocoding_info(geocode_data, building_components, street_components, exclude_components)
            building_info_list.append(", ".join(building_info))
            street_info_list.append(", ".join(street_info))
        else:
            building_info_list.append(address)
            street_info_list.append("")
    
    dataset['BuildingInfo'] = building_info_list
    dataset['StreetInfo'] = street_info_list
    return dataset

def merge_datasets(file1, file2, output_file):
    """
    Combines two datasets into one and saves the result.
    """
    data1 = pd.read_csv(file1)
    data2 = pd.read_csv(file2)
    combined_data = pd.concat([data1, data2], ignore_index=True)
    combined_data.to_csv(output_file, index=False)
    print(f"Datasets combined and saved to {output_file}")

# Define paths and required columns
file_path = "5000 INDIA lei-records.csv"
output_path = "New_Dataset.csv"
split_output1 = "Part1.csv"
split_output2 = "Part2.csv"
final_output = "final_output.csv"
required_columns = [
    "LEI", "LegalName", "Entity.LegalAddress.FirstAddressLine",
    "Entity.LegalAddress.AdditionalAddressLine.1", "Entity.LegalAddress.AdditionalAddressLine.2",
    "Entity.LegalAddress.AdditionalAddressLine.3", "Entity.LegalAddress.City",
    "Entity.LegalAddress.Region", "Entity.LegalAddress.Country", "Entity.LegalAddress.PostalCode"
]
address_columns = [
    "Entity.LegalAddress.FirstAddressLine",
    "Entity.LegalAddress.AdditionalAddressLine.1",
    "Entity.LegalAddress.AdditionalAddressLine.2",
    "Entity.LegalAddress.AdditionalAddressLine.3"
]

# Process and split the CSV file
processed_data = process_csv(file_path, output_path, required_columns)
split_dataset(output_path, split_output1, split_output2)

# Concatenate address components and process geocoding
part1 = pd.read_csv(split_output1)
part2 = pd.read_csv(split_output2)
part1 = concatenate_address_components(part1, address_columns)
part2 = concatenate_address_components(part2, address_columns)

part1 = process_addresses(part1, 'FullAddress')
part2 = process_addresses(part2, 'FullAddress')

# Save processed datasets
part1.to_csv('dataset1_with_full_address.csv', index=False)
part2.to_csv('dataset2_with_full_address.csv', index=False)

# Merge final datasets
merge_datasets('dataset1_with_full_address.csv', 'dataset2_with_full_address.csv', final_output)