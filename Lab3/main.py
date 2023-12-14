import json
import os
import csv
import boto3

def find_files():
    json_data = []
    filenames = []
    for root, dirs, files in os.walk("data"):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)

                with open(file_path, 'r') as json_file:
                    try:
                        data = json.load(json_file)
                        json_data.append(data)
                        filenames.append(file.split(".")[0])
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON in {file_path}: {e}")
    return [json_data, filenames]

def format_json_data(json_data):
    def process_value(value, field_name, formatted_dict):
        if isinstance(value, list):
            for i, item in enumerate(value):
                new_key = f"{field_name}_{i}"
                formatted_dict[new_key] = item
        else:
            formatted_dict[field_name] = value

    def format_dict(d, formatted_dict=None, prefix=""):
        if formatted_dict is None:
            formatted_dict = {}
        for key, value in d.items():
            new_key = f"{prefix}{key}"
            if isinstance(value, dict):
                format_dict(value, formatted_dict, prefix=new_key+"_")
            else:
                process_value(value, new_key, formatted_dict)
        return formatted_dict

    if isinstance(json_data, dict):
        return format_dict(json_data)
    else:
        return json_data

def main():
    data, filenames = find_files()
    formatted_data = [format_json_data(data) for data in data]
    print(data)
    print(formatted_data)

    for i in range(len(formatted_data)):
        with open(filenames[i] + ".csv", 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(formatted_data[i].keys())
            csv_writer.writerow(formatted_data[i].values())
    pass


if __name__ == "__main__":
    main()
