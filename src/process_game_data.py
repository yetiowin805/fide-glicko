import asyncio
import aiofiles
import logging
from pathlib import Path
import argparse
import json
import re
from collections import defaultdict

async def load_tournament_player_mapping(data_dir, time_control, tournament_id):
    """
    Load player information from a tournament file and build a name to ID mapping.
    
    Args:
        data_dir: Base data directory
        time_control: Time control category (standard, rapid, blitz)
        tournament_id: Tournament ID
        
    Returns:
        dict: Mapping from cleaned player names to FIDE IDs
    """
    name_to_id = {}
    tournament_file_pattern = f"**/{tournament_id}.txt"
    tournament_files = list(Path(data_dir).glob(tournament_file_pattern))
    
    # Take the first matching file
    tournament_file = tournament_files[0]
    
    try:
        async with aiofiles.open(tournament_file, "r", encoding="utf-8") as f:
            async for line in f:
                if not line.strip():
                    continue
                
                player_data = json.loads(line)
                if "id" in player_data and "name" in player_data:
                    player_id = str(player_data["id"])
                    player_name = player_data["name"]
                    name_to_id[player_name] = player_id
        
        logging.info(f"Loaded {len(name_to_id)} players for tournament {tournament_id}")
    except Exception as e:
        logging.error(f"Error loading tournament file {tournament_file}: {e}")
    
    return name_to_id


async def process_calculation_files(data_dir, month):
    """
    Process calculation files and generate clean numerical data.
    Now also uses tournament-specific player mappings.
    
    Args:
        data_dir: Base data directory
        month: Month to process in YYYY-MM format
    """
    calculations_dir = Path(data_dir) / "calculations" / month
    output_dir = Path(data_dir) / "clean_numerical" / month
    processed_tournament_dir = Path(data_dir) / "processed_tournament_data"

    # Dictionary to store games by time control and player
    # Structure: {time_control: {player_id: [(opponent_id, result), ...]}}
    games_by_tc_player = defaultdict(lambda: defaultdict(list))

    if not calculations_dir.exists():
        logging.error(f"Calculations directory not found: {calculations_dir}")
        return

    # Process each time control directory
    for tc_dir in calculations_dir.iterdir():
        if not tc_dir.is_dir():
            continue
            
        time_control = tc_dir.name  # standard, rapid, or blitz
        
        # Process each player file
        for player_file in tc_dir.iterdir():
            if not player_file.suffix == '.json':
                continue
                
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
                    
                    # Load player mapping for this specific tournament
                    name_to_id = await load_tournament_player_mapping(
                        processed_tournament_dir, time_control, tournament_id
                    )
                    
                    # Process each game in this tournament
                    for game in tournament.get('games', []):
                        opponent_name = game.get('opponent_name', '')
                        result = float(game.get('result', ''))
                        
                        # Clean the opponent name
                        opponent_name = opponent_name
                        
                        # Look up the opponent's FIDE ID, first in tournament mapping, then in global mapping
                        opponent_id = None
                        if opponent_name in name_to_id:
                            opponent_id = name_to_id[opponent_name]
                        
                        if opponent_id:
                            # Convert the result to a numeric value
                            if result == 1:
                                numeric_result = '1.0'
                            elif result == 0:
                                numeric_result = '0.0'
                            elif result == 0.5:
                                numeric_result = '0.5'
                            else:
                                logging.warning(f"Unknown result format: {result}")
                                numeric_result = result
                            
                            # Add to the collection grouped by player
                            games_by_tc_player[time_control][player_id].append((opponent_id, numeric_result))
                        else:
                            logging.warning(f"{player_id}: Could not find ID for opponent: '{opponent_name}' in tournament {tournament_id}")
            except Exception as e:
                logging.error(f"Error processing calculation file {player_file}: {e}")

    # Write the output files in the new format
    for time_control, players_games in games_by_tc_player.items():
        output_path = output_dir / f"{time_control}.txt"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            async with aiofiles.open(output_path, "w", encoding="utf-8") as f:
                # Sort players for consistent output
                for player_id in sorted(players_games.keys()):
                    games = players_games[player_id]
                    # Write the player ID and number of games
                    await f.write(f"{player_id} {len(games)}\n")
                    # Write each opponent and result
                    for opponent_id, result in games:
                        await f.write(f"{opponent_id} {result}\n")

            # Count total games for logging
            total_games = sum(len(games) for games in players_games.values())
            logging.info(
                f"Wrote {total_games} games for {len(players_games)} players to {output_path}"
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
