import os
import argparse
import logging
from pathlib import Path
import asyncio
import aiofiles
import json
from datetime import datetime


async def read_player_ids_from_file(file_path):
    """Read player IDs from a single tournament file in JSON format."""
    try:
        player_ids = []
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    # Parse the JSON object and extract the id field
                    player_data = json.loads(line)
                    if "id" in player_data:
                        player_ids.append(player_data["id"])
                except json.JSONDecodeError as e:
                    logging.warning(f"Invalid JSON in {file_path}: {line}")
                    continue

        return player_ids
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return []


async def process_directory(directory_path):
    """Process all tournament files in a directory and its subdirectories."""
    all_player_ids = set()
    directory = Path(directory_path)

    # Find all .txt files recursively
    all_files = list(directory.glob("**/*.txt"))
    logging.info(f"Found {len(all_files)} tournament files to process")

    # Process files in chunks to avoid opening too many files at once
    chunk_size = 1000
    for i in range(0, len(all_files), chunk_size):
        chunk = all_files[i : i + chunk_size]
        tasks = [read_player_ids_from_file(file) for file in chunk]
        results = await asyncio.gather(*tasks)

        # Add all player IDs to the set
        for player_ids in results:
            all_player_ids.update(player_ids)

        logging.info(
            f"Processed {min(i+chunk_size, len(all_files))}/{len(all_files)} files, found {len(all_player_ids)} unique player IDs so far"
        )

    return all_player_ids


async def main(month, data_dir):
    """Main function to process all tournament files and create a master list of player IDs."""
    month_str = month  # Format: YYYY-MM

    # Define paths
    base_path = Path(data_dir)
    processed_data_path = base_path / "processed_tournament_data"
    output_dir = base_path / "active_players"

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if processed data path exists
    if not processed_data_path.exists():
        logging.error(
            f"Processed tournament data directory not found: {processed_data_path}"
        )
        return

    # Time control categories
    time_controls = ["standard", "rapid", "blitz"]
    year = int(month_str.split("-")[0])

    for time_control in time_controls:
        # Process specific time control
        tc_path = processed_data_path / time_control
        if not tc_path.exists():
            logging.info(f"No {time_control} tournaments found")
            continue

        # Initialize a set to collect all unique player IDs for this time control
        unique_player_ids = set()
        tournaments_processed = 0

        # Process only the directory for the current month
        month_dir = tc_path / month_str
        if not month_dir.exists() or not month_dir.is_dir():
            logging.info(f"No {time_control} tournaments found for {month_str}")
            continue

        logging.info(f"Processing {time_control} tournaments from {month_str}")

        # Process all tournament files in this month directory
        for tournament_file in month_dir.glob("*.txt"):
            player_ids = await read_player_ids_from_file(tournament_file)
            unique_player_ids.update(player_ids)
            tournaments_processed += 1

            if tournaments_processed % 100 == 0:
                logging.info(
                    f"Processed {tournaments_processed} tournaments for {time_control}, found {len(unique_player_ids)} unique player IDs so far"
                )

        # Sort the IDs numerically
        sorted_ids = sorted(unique_player_ids, key=int)

        # Skip if no player IDs found
        if not sorted_ids:
            logging.info(f"No player IDs found for {time_control} in {month_str}")
            continue

        # Write the sorted, unique IDs to the output file
        output_file = output_dir / f"{month_str}_{time_control}.txt"
        async with aiofiles.open(output_file, "w", encoding="utf-8") as f:
            await f.write("\n".join(sorted_ids))

        logging.info(
            f"Successfully saved {len(sorted_ids)} unique player IDs to {output_file} from {tournaments_processed} tournaments"
        )


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Aggregate player IDs from processed tournament files into a master list"
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--data_dir", type=str, help="Base directory for all data files", default="."
    )

    args = parser.parse_args()

    # Run the main asynchronous function
    asyncio.run(main(args.month, args.data_dir))
