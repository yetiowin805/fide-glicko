def process_line(line, lengths):
    """Splits a line into parts of fixed lengths."""
    parts = []
    start = 0
    for length in lengths:
        part = line[start:start+length].strip()
        parts.append(part)
        start += length
    return parts

def line_to_json(parts, keys):
    """Converts parts of a line into a JSON object."""
    return {key: value for key, value in zip(keys, parts)}

def process_file(input_filename, output_filename, lengths, keys):
    """Reads an input file, processes each line, and writes the result as JSON objects to an output file."""
    with open(input_filename, 'r', encoding='utf-8', errors='replace') as input_file, open(output_filename, 'w') as output_file:
        input_file.readline()
        for line in input_file:
            parts = process_line(line, lengths)
            json_object = line_to_json(parts, keys)
            output_file.write(f"{json.dumps(json_object)}\n")

# Example usage
import datetime

start_date = datetime.date(2001, 4, 1)
end_date = datetime.date(2001, 4, 1)

current_date = start_date
import json
from dateutil.relativedelta import relativedelta
lengths = [12,33,6,8,6,6,11,4] # Example fixed lengths for each part of the line
keys = ["id", "name","title","fed","rating","games","b_year","flag"] # Keys for the JSON objects
while current_date <= end_date:
    month_year = current_date.strftime("%Y-%m")
    input_filename = f"./player_info/raw/{month_year}.txt"
    print(input_filename)
    output_filename = f"./player_info/processed/{month_year}.txt"
    process_file(input_filename, output_filename, lengths, keys)
    current_date += relativedelta(months=3)
