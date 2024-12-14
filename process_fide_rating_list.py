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
        # Extract year and month from filename
        filename = os.path.basename(input_filename)
        year, month = map(int, filename.split(".")[0].split("-"))

        # Skip header line only for certain dates
        if not (
            (year == 2003 and month in [7, 10])
            or (year == 2004 and month == 1)
            or (year == 2005 and month == 4)
        ):
            input_file.readline()

        for line in input_file:
            parts = process_line(line, lengths)
            json_object = {key: value for key, value in zip(keys, parts)}
            output_file.write(f"{json.dumps(json_object)}\n")


def get_format_config(year, month):
    """
    Determine the format configuration based on the date.
    Returns a tuple of (lengths, keys) for the given year and month.
    """
    if year == 2001:
        if month <= 4:
            return (
                [10, 33, 6, 8, 6, 6, 11, 4],
                ["id", "name", "title", "fed", "rating", "games", "b_year", "flag"],
            )
        else:
            return (
                [10, 33, 6, 8, 6, 6, 11, 4, 8],
                [
                    "id",
                    "name",
                    "title",
                    "fed",
                    "rating",
                    "games",
                    "b_year",
                    "sex",
                    "flag",
                ],
            )

    if year == 2002 and month < 10:
        if month == 4:
            return (
                [10, 33, 6, 8, 6, 6, 11, 4, 6],
                [
                    "id",
                    "name",
                    "title",
                    "fed",
                    "rating",
                    "games",
                    "b_year",
                    "sex",
                    "flag",
                ],
            )
        return (
            [10, 33, 6, 8, 6, 6, 11, 6],
            ["id", "name", "title", "fed", "rating", "games", "b_year", "flag"],
        )
    if year < 2005 or (year == 2005 and month <= 7):
        return (
            [9, 32, 6, 8, 6, 5, 11, 4],
            ["id", "name", "title", "fed", "rating", "games", "b_year", "flag"],
        )
    if year < 2012 or (year == 2012 and month <= 8):
        return (
            [10, 32, 6, 4, 6, 4, 6, 4],
            ["id", "name", "title", "fed", "rating", "games", "b_year", "flag"],
        )
    if year < 2016 or (year == 2016 and month <= 8):
        return (
            [15, 61, 4, 4, 5, 5, 15, 6, 4, 3, 6, 4],
            [
                "id",
                "name",
                "fed",
                "sex",
                "title",
                "w_title",
                "o_title",
                "rating",
                "games",
                "k",
                "b_year",
                "flag",
            ],
        )

    return (
        [15, 61, 4, 4, 5, 5, 15, 4, 6, 4, 3, 6, 4],
        [
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
        ],
    )


def main(month):
    year, month = map(int, month.split("-"))
    lengths, keys = get_format_config(year, month)

    month_year = f"{year:04d}-{month:02d}"
    input_filename = f"./player_info/raw/{month_year}.txt"
    output_filename = f"./player_info/processed/{month_year}.txt"

    if os.path.exists(input_filename):
        print(f"Processing file: {input_filename}")
        process_file(input_filename, output_filename, lengths, keys)
    else:
        print(f"File not found: {input_filename}. Skipping...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process player information for a specific month."
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
        required=True,
    )

    args = parser.parse_args()
    main(args.month)
