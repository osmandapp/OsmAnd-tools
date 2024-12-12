import csv
import re
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import multiprocessing

def compile_replacement_pattern(abbreviations):
    """
    Creates a single regular expression to replace all abbreviations with full names
    """
    pattern = r'\b(' + '|'.join(re.escape(abbr) for abbr in abbreviations.keys()) + r')\b'
    replacements = {abbr: full_name.capitalize() for abbr, full_name in abbreviations.items()}
    return pattern, replacements

def replace_abbreviations_in_line(line, pattern, replacements):
    """
    Replaces abbreviations with full names in a string using a single regular expression
    """
    def replacement_func(match):
        abbr = match.group(0)
        return replacements[abbr.lower()]
    
    return re.sub(pattern, replacement_func, line, flags=re.IGNORECASE)

def process_line_osm(line, pattern, replacements):
    """
    Processes an OSM string: replaces abbreviations with full names if the string contains the addr:street tag
    """
    if 'addr:street' in line:
        return replace_abbreviations_in_line(line, pattern, replacements)
    return line

def process_csv_row(row, street_column, pattern, replacements):
    """
    Processes a CSV string: replaces abbreviations in the street column
    """
    if street_column in row:
        row[street_column] = replace_abbreviations_in_line(row[street_column], pattern, replacements)
    return row

def process_osm_file(input_file, output_file, pattern, replacements, buffer_size=50 * 1024 * 1024):
    """
    Processes OSM XML file with abbreviation replacement
    """
    # Opening input and output files
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        total_lines = sum(1 for _ in infile)
        infile.seek(0)  # Reset the pointer to the beginning of the file

        pbar = tqdm(total=total_lines, desc="Обработка строк OSM", unit="строка")

        current_size = 0
        buffer = []

        # Reading and processing strings
        for line in infile:
            processed_line = process_line_osm(line, pattern, replacements)
            buffer.append(processed_line)
            current_size += len(processed_line.encode('utf-8'))

            if current_size >= buffer_size:
                outfile.writelines(buffer)
                buffer.clear()
                current_size = 0

            pbar.update(1)

        if buffer:
            outfile.writelines(buffer)

        pbar.close()

def process_csv_file(input_file, output_file, pattern, replacements, buffer_size=50 * 1024 * 1024):
    """
    Processes a CSV file with replacement of abbreviations in the street column, flushing data to disk every 50 MB
    """
    # Count the total number of rows in the input CSV file
    total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8')) - 1  # -1 for exclude row with header

    with open(input_file, 'r', newline='', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile, delimiter=';')
        fieldnames = reader.fieldnames

        if "street" not in fieldnames:
            raise ValueError("The input CSV file must contain a 'street' column")

        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()

        # Update tqdm by adding the total number of rows
        pbar = tqdm(total=total_lines, desc="Обработка строк CSV", unit="строка", unit_scale=True)

        buffer = []
        current_size = 0

        for row in reader:
            # Process current row
            processed_row = process_csv_row(row, "street", pattern, replacements)
            
            # Convert string to CSV and push to bufer
            csv_line = ";".join([processed_row[field] for field in fieldnames]) + "\n"
            buffer.append(csv_line)
            current_size += len(csv_line.encode('utf-8'))

            # If the buffer size reaches 50 MB, flush to disk
            if current_size >= buffer_size:
                outfile.writelines(buffer)
                buffer.clear()
                current_size = 0

            pbar.update(1)

        # Write the remaining data
        if buffer:
            outfile.writelines(buffer)

        pbar.close()

def process_file(input_file, csv_file, output_file, buffer_size=50 * 1024 * 1024):
    """
    Determines the input data file type (OSM or CSV) and processes it
    """
    # Reading CSV file with abbreviations
    abbreviations = {}
    with open(csv_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            full_name, abbr = row
            abbreviations[abbr.strip()] = full_name.strip()

    # Creating a Regular Expression and Replacement Dictionary
    pattern, replacements = compile_replacement_pattern(abbreviations)

    # Checking the input file type
    if input_file.endswith('.osm'):
        process_osm_file(input_file, output_file, pattern, replacements, buffer_size)
    elif input_file.endswith('.csv'):
        process_csv_file(input_file, output_file, pattern, replacements)
    else:
        raise ValueError("Invalid input file format. Only '.osm' and '.csv' are supported")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python replace_abbreviations.py <input_file> <abbreviations.csv> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    csv_file = sys.argv[3]

    process_file(input_file, csv_file, output_file)