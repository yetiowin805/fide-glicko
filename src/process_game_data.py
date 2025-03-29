import asyncio
import aiofiles
import logging
from pathlib import Path
import argparse
import json
import re
from collections import defaultdict

# Cache for tournament player mappings {tournament_id: name_to_id_map}
tournament_map_cache = {}
# Cache for global player mappings {month: name_to_id_map}
global_player_map_cache = {}
async def load_global_player_mapping(data_dir, month):
    """
    Load the global player mapping from the master FIDE player list.
    Uses a cache to avoid reloading the same file.

    Args:
        data_dir: Base data directory
        month: Month in YYYY-MM format

    Returns:
        dict: Mapping from player names to FIDE IDs
    """
    # Check cache first
    if month in global_player_map_cache:
        return global_player_map_cache[month]

    name_to_id = {}
    global_player_file = Path(data_dir) / "player_info" / "processed" / f"{month}.txt"

    if not global_player_file.exists():
        logging.warning(f"Global player file not found: {global_player_file}")
        # Cache empty result to avoid retrying
        global_player_map_cache[month] = {}
        return {}

    try:
        # Track names we've seen to detect duplicates
        seen_names = set()
        duplicate_names = set()
        
        async with aiofiles.open(global_player_file, "r", encoding="utf-8") as f:
            content = await f.read()
            lines = content.splitlines()
            for line in lines:
                if not line.strip():
                    continue
                try:
                    player_data = json.loads(line)
                    if "id" in player_data and "name" in player_data:
                        player_id = str(player_data["id"])
                        player_name = player_data["name"]
                        
                        # Normalize the name by removing capitalization, commas, and periods
                        normalized_name = player_name.lower().replace(",", "").replace(".", "")
                        
                        # Check for duplicate names
                        if normalized_name in seen_names:
                            duplicate_names.add(normalized_name)
                        else:
                            seen_names.add(normalized_name)
                            name_to_id[normalized_name] = player_id
                except json.JSONDecodeError as json_e:
                    logging.warning(f"Skipping invalid JSON line in {global_player_file}: {line} - Error: {json_e}")
                    continue
        
        # Remove all entries for duplicate names
        for name in duplicate_names:
            if name in name_to_id:
                del name_to_id[name]
        
        if duplicate_names:
            logging.warning(f"Removed {len(duplicate_names)} duplicate player names from mapping")

        logging.info(f"Loaded {len(name_to_id)} players from global player list for {month}")
        # Store in cache
        global_player_map_cache[month] = name_to_id
    except Exception as e:
        logging.error(f"Error loading global player file {global_player_file}: {e}")
        global_player_map_cache[month] = {}

    return global_player_map_cache[month]

async def load_tournament_player_mapping(data_dir, time_control, tournament_id):
    """
    Load player information from a tournament file and build a name to ID mapping.
    Uses a cache to avoid reloading the same file.

    Args:
        data_dir: Base data directory for processed tournament data
        time_control: Time control category (standard, rapid, blitz) - currently unused but kept for signature consistency
        tournament_id: Tournament ID

    Returns:
        dict: Mapping from cleaned player names to FIDE IDs, or empty dict on failure.
    """
    # Check cache first
    if tournament_id in tournament_map_cache:
        # logging.debug(f"Cache hit for tournament {tournament_id}")
        return tournament_map_cache[tournament_id]

    # logging.debug(f"Cache miss for tournament {tournament_id}, loading from file.")
    name_to_id = {}
    # Construct the expected path directly instead of globbing each time
    # Assumes a structure like data_dir / tournament_id.txt
    tournament_file = Path(data_dir) / f"{tournament_id}.txt"

    if not tournament_file.exists():
         # Try globbing as a fallback if direct path fails (original behavior)
        tournament_file_pattern = f"**/{tournament_id}.txt"
        tournament_files = list(Path(data_dir).glob(tournament_file_pattern))
        if not tournament_files:
            logging.warning(f"Tournament file not found for ID: {tournament_id} in {data_dir}")
            # Cache the fact that it's not found
            tournament_map_cache[tournament_id] = {}
            return {}
        tournament_file = tournament_files[0] # Take the first match

    try:
        async with aiofiles.open(tournament_file, "r", encoding="utf-8") as f:
            # Read the whole file at once, might be faster for smaller files
            content = await f.read()
            lines = content.splitlines()
            for line in lines:
                if not line.strip():
                    continue
                try:
                    player_data = json.loads(line)
                    if "id" in player_data and "name" in player_data:
                        player_id = str(player_data["id"])
                        player_name = player_data["name"]
                        # TODO: Consider adding name cleaning here if needed
                        name_to_id[player_name] = player_id
                except json.JSONDecodeError as json_e:
                     logging.warning(f"Skipping invalid JSON line in {tournament_file}: {line} - Error: {json_e}")
                     continue # Skip malformed lines

        # logging.info(f"Loaded {len(name_to_id)} players for tournament {tournament_id}")
        # Store in cache
        tournament_map_cache[tournament_id] = name_to_id
    except Exception as e:
        logging.error(f"Error loading tournament file {tournament_file}: {e}")
        # Cache empty dict on error to prevent retrying failed loads repeatedly
        tournament_map_cache[tournament_id] = {}

    return tournament_map_cache[tournament_id] # Return from cache


async def process_calculation_files(data_dir, month):
    """
    Process calculation files and generate clean numerical data.
    Now also uses tournament-specific player mappings with caching.

    Args:
        data_dir: Base data directory
        month: Month to process in YYYY-MM format
    """
    calculations_dir = Path(data_dir) / "calculations" / month
    output_dir = Path(data_dir) / "clean_numerical" / month
    processed_tournament_dir = Path(data_dir) / "processed_tournament_data"

    # Clear cache at the start of processing a month to manage memory
    global tournament_map_cache, global_player_map_cache
    tournament_map_cache.clear()
    global_player_map_cache.clear()
    logging.info("Cleared tournament and global player map caches.")
    
    # Load the global player mapping for fallback
    # Calculate the previous month
    from datetime import datetime
    current_month = datetime.strptime(month, "%Y-%m")
    if current_month.month == 1:
        prev_month = f"{current_month.year-1}-12"
    else:
        prev_month = f"{current_month.year}-{current_month.month-1:02d}"
    
    global_player_map = await load_global_player_mapping(data_dir, prev_month)
    logging.info(f"Loaded global player mapping with {len(global_player_map)} entries")

    # Dictionary to store games by time control and player
    # Structure: {time_control: {player_id: [(opponent_id, result), ...]}}
    games_by_tc_player = defaultdict(lambda: defaultdict(list))

    if not calculations_dir.exists():
        logging.error(f"Calculations directory not found: {calculations_dir}")
        return

    # Process each time control directory (sequentially)
    for tc_dir in calculations_dir.iterdir():
        if not tc_dir.is_dir():
            continue

        time_control = tc_dir.name  # standard, rapid, or blitz
        logging.info(f"Processing time control: {time_control}") # Added logging

        # Process each player file (sequentially)
        player_files = list(tc_dir.glob('*.json')) # Get files first for logging count
        logging.info(f"Found {len(player_files)} player files for {time_control}.")
        processed_files_count = 0

        for player_file in player_files:
            # Removed check: if not player_file.suffix == '.json': continue (glob already filters)

            player_id = player_file.stem  # The FIDE ID of the player

            try:
                # Load the calculation data
                async with aiofiles.open(player_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    calculation_data = json.loads(content)

                # Process each tournament in the calculation
                for tournament in calculation_data.get('tournaments', []):
                    tournament_id = tournament.get('tournament_id')
                    if not tournament_id:
                        continue

                    # Load player mapping for this specific tournament (will use cache)
                    name_to_id = await load_tournament_player_mapping(
                        processed_tournament_dir, time_control, tournament_id
                    )
                    if not name_to_id: # Skip if mapping failed to load or was empty
                         # logging.warning(f"Skipping tournament {tournament_id} for player {player_id} due to missing/failed mapping.") # Optional: reduce noise
                         continue

                    # Process each game in this tournament
                    for game in tournament.get('games', []):
                        opponent_name = game.get('opponent_name', '')
                        try:
                            result = float(game.get('result', '')) # Ensure result is float
                        except (ValueError, TypeError):
                             logging.warning(f"Invalid result format: {game.get('result')} for player {player_id} vs {opponent_name} in tournament {tournament_id}. Skipping game.")
                             continue # Skip game if result is invalid

                        # Clean the opponent name (Placeholder)
                        # opponent_name = clean_name(opponent_name)

                        # Look up the opponent's FIDE ID using the tournament mapping
                        opponent_id = name_to_id.get(opponent_name) # Use .get for safer lookup

                        # Fall back to global player mapping if not found in tournament
                        if not opponent_id and opponent_name:
                            # logging.warning(f"{player_id}: Could not find ID for opponent: '{opponent_name}' in tournament {tournament_id}")
                            # Normalize the name by removing capitalization, commas, and periods
                            normalized_name = opponent_name.lower().replace(",", "").replace(".", "")
                            opponent_id = global_player_map.get(normalized_name)
                        if not opponent_id:
                            logging.warning(f"{player_id}: Could not find ID for opponent: '{opponent_name}' in global mapping")

                        if opponent_id:
                            # Convert the result to a numeric value
                            if result not in [0.0, 0.5, 1.0]:
                                logging.warning(f"Unknown numeric result value: {result} for player {player_id} vs {opponent_id}. Assuming valid.")
                                # Decide how to handle unexpected float values if necessary

                            # Add to the collection grouped by player
                            games_by_tc_player[time_control][player_id].append((opponent_id, result))

                            # Add game from opponent's perspective if the player was unrated
                            if tournament.get("player_is_unrated", False): # Default to False
                                opponent_id_str = str(opponent_id) # Ensure string key
                                games_by_tc_player[time_control][opponent_id_str].append((player_id, 1.0 - result))
                        else:
                            logging.warning(f"{player_id}: Could not find ID for opponent: '{opponent_name}' in tournament {tournament_id}")
                processed_files_count += 1 # Count successfully opened/parsed files

            except json.JSONDecodeError as json_e:
                 logging.error(f"Error decoding JSON from calculation file {player_file}: {json_e}")
                 # Continue to next file
            except Exception as e:
                logging.error(f"Error processing calculation file {player_file}: {e}")
                # Continue to next file

        logging.info(f"Processed {processed_files_count} calculation files for {time_control}.") # Log count

    # Write the output files in the new format
    logging.info("Writing output files...") # Added logging
    for time_control, players_games in games_by_tc_player.items():
        if not players_games: # Skip writing if no games for this TC
            logging.info(f"No games to write for time control {time_control}.")
            continue

        output_path = output_dir / f"{time_control}.txt"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            async with aiofiles.open(output_path, "w", encoding="utf-8") as f:
                # Sort players for consistent output
                sorted_player_ids = sorted(players_games.keys())
                logging.info(f"Writing {len(sorted_player_ids)} players' games for {time_control} to {output_path}...")

                for player_id in sorted_player_ids:
                    games = players_games[player_id]
                    # Write the player ID and number of games
                    await f.write(f"{player_id} {len(games)}\n")
                    # Write each opponent and result
                    # Sort games by opponent ID for deterministic output (optional)
                    # games.sort(key=lambda x: x[0])
                    for opponent_id, result in games:
                        # Ensure result is formatted correctly (e.g., 0.5 not 0.500000...)
                        result_str = f"{result:.1f}" if result == 0.5 else str(int(result))
                        await f.write(f"{opponent_id} {result_str}\n")

            # Count total games for logging
            total_games = sum(len(games) for games in players_games.values())
            logging.info(
                f"Wrote {total_games} game entries for {len(players_games)} players to {output_path}"
            )
        except Exception as e:
            logging.error(f"Error writing to {output_path}: {e}")


async def main(month, data_dir):
    """
    Main function to coordinate the processing of calculation data.

    Args:
        month: Month to process in YYYY-MM format
        data_dir: Base directory for all data files
    """

    # Process calculation files
    logging.info(f"Processing calculation files for {month}")
    await process_calculation_files(data_dir, month)

    logging.info("Processing complete")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Process FIDE calculation data into clean numerical format"
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
