import os
import json
import argparse


def process_line(line, lengths):
    """Splits a line into parts of fixed lengths."""
    parts = []
    start = 0
    for length in lengths:
        part = line[start : start + length].strip()
        parts.append(part)
        start += length
    return parts


def process_file(input_filename, output_filename, lengths, keys):
    """Reads an input file, processes each line, and writes the result as JSON objects to an output file."""
    with open(
        input_filename, "r", encoding="utf-8", errors="replace"
    ) as input_file, open(output_filename, "w") as output_file:
        input_file.readline()  # Skip header line
        for line in input_file:
            parts = process_line(line, lengths)
            json_object = {key: value for key, value in zip(keys, parts)}
            output_file.write(f"{json.dumps(json_object)}\n")


def main(start_month, end_month):
    start_year, start_month = map(int, start_month.split("-"))
    end_year, end_month = map(int, end_month.split("-"))

    lengths = [
        15,
        61,
        4,
        4,
        5,
        5,
        15,
        4,
        6,
        4,
        3,
        6,
        4,
    ]
    keys = [
        "id",
        "name",
        "fed",
        "sex",
        "title",
        "w_title",
        "o_title",
        "foa",
        "rating",
        "games",
        "k",
        "b_year",
        "flag",
    ]

    current_year, current_month = start_year, start_month

    while (current_year < end_year) or (
        current_year == end_year and current_month <= end_month
    ):
        month_year = f"{current_year:04d}-{current_month:02d}"
        input_filename = f"./player_info/raw/{month_year}.txt"
        output_filename = f"./player_info/processed/{month_year}.txt"

        # Check if file exists and process
        if os.path.exists(input_filename):
            print(f"Processing file: {input_filename}")
            process_file(input_filename, output_filename, lengths, keys)
        else:
            print(f"File not found: {input_filename}. Skipping...")

        # Increment the month
        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process player information from a certain month range."
    )
    parser.add_argument(
        "--start_month",
        type=str,
        help="Start month for processing in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--end_month",
        type=str,
        help="End month for processing in YYYY-MM format",
        required=True,
    )

    args = parser.parse_args()
    main(args.start_month, args.end_month)
