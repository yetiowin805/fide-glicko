import sys
from tqdm import tqdm
import math
import os
from countries import countries
import argparse
import logging

# Constants moved to top and grouped logically
RATING_CONSTANTS = {
    "BASE_RATING": 1500.0,
    "BASE_RD": 350.0,
    "BASE_VOLATILITY": 0.09,
    "TAU": 0.2,
    "SCALE": 173.7178,
    "MAX_RD": 500,
    "MAX_VOLATILITY": 0.1,
}

MATH_CONSTANTS = {"PI_SQUARED": math.pi ** 2}

FEDERATIONS = countries


class GameResult:
    """Represents the result of a game between two players."""

    def __init__(
        self, opponent_id=None, score=None, opponent_rating=None, opponent_rd=None
    ):
        if opponent_id is not None and score is not None:
            self.opponent_id = opponent_id
            self.score = score
            self.generatedGame = False
        elif (
            opponent_rating is not None
            and opponent_rd is not None
            and score is not None
        ):
            self.opponent_rating = opponent_rating
            self.opponent_rd = opponent_rd
            self.score = score
            self.generatedGame = True
        else:
            raise ValueError("Invalid arguments for GameResult")


class Player:
    """Represents a player with Glicko-2 rating attributes."""

    def __init__(
        self,
        player_id,
        rating=RATING_CONSTANTS["BASE_RATING"],
        rd=RATING_CONSTANTS["BASE_RD"],
        volatility=RATING_CONSTANTS["BASE_VOLATILITY"],
    ):
        self.id = player_id
        self.rating = rating
        self.rd = rd
        self.volatility = volatility
        self.new_rating = rating
        self.new_rd = rd
        self.games = []


def add_game(player_id, game, players_dict):
    player = players_dict.setdefault(player_id, Player(player_id))
    if not game.generatedGame:
        opponent = players_dict.setdefault(game.opponent_id, Player(game.opponent_id))
    player.games.append(game)


def f(x, delta, v, A):
    ex = math.exp(x)
    ex_v_sum = v + ex
    return (ex * (delta ** 2 - v - ex)) / (2 * ex_v_sum ** 2) - (
        x - A
    ) / RATING_CONSTANTS["TAU"] ** 2


def glicko2_update(target, players):
    # Add early return for no games
    if not target.games:
        phi = target.rd / RATING_CONSTANTS["SCALE"]
        phi_star = math.sqrt(phi ** 2 + target.volatility ** 2)
        target.new_rd = min(
            phi_star * RATING_CONSTANTS["SCALE"], RATING_CONSTANTS["MAX_RD"]
        )
        return

    mu = (target.rating - RATING_CONSTANTS["BASE_RATING"]) / RATING_CONSTANTS["SCALE"]
    phi = target.rd / RATING_CONSTANTS["SCALE"]

    v_inv = 0
    delta_sum = 0

    for game in target.games:
        if not game.generatedGame:
            opponent = players[game.opponent_id]
            mu_j = (
                opponent.rating - RATING_CONSTANTS["BASE_RATING"]
            ) / RATING_CONSTANTS["SCALE"]
            phi_j = opponent.rd / RATING_CONSTANTS["SCALE"]
        else:
            mu_j = (
                game.opponent_rating - RATING_CONSTANTS["BASE_RATING"]
            ) / RATING_CONSTANTS["SCALE"]
            phi_j = game.opponent_rd / RATING_CONSTANTS["SCALE"]

        g_phi_j = 1.0 / math.sqrt(
            1.0 + (3.0 * phi_j ** 2) / MATH_CONSTANTS["PI_SQUARED"]
        )

        e_val = 1.0 / (1.0 + math.exp(-g_phi_j * (mu - mu_j)))

        v_inv += g_phi_j ** 2 * e_val * (1 - e_val)
        delta_sum += g_phi_j * (game.score - e_val)

    v = 1.0 / v_inv

    delta = v * delta_sum

    a = math.log(target.volatility ** 2)
    A = a
    if delta ** 2 > phi ** 2 + v:
        B = math.log(delta ** 2 - phi ** 2 - v)
    else:
        k = 1
        while f(a - k * RATING_CONSTANTS["TAU"], delta, v, a) < 0:
            k += 1
        B = a - k * RATING_CONSTANTS["TAU"]

    epsilon = 0.000001
    fa = f(A, delta, v, a)
    fb = f(B, delta, v, a)

    counter = 0

    while math.fabs(B - A) > epsilon:
        C = A + (A - B) * fa / (fb - fa)
        fc = f(C, delta, v, a)

        if fc * fb < 0:
            A = B
            fa = fb
        else:
            fa /= 2
        B = C
        fb = fc

        counter += 1
        if counter > 1000:
            break

    new_volatility = math.exp(A / 2.0)
    phi_star = math.sqrt(phi ** 2 + new_volatility ** 2)
    new_phi = 1.0 / math.sqrt(1.0 / phi_star ** 2 + 1.0 / v)
    if new_phi ** 2 * delta_sum > 1000.0 / RATING_CONSTANTS["SCALE"]:
        new_mu = mu + 1000.0 / RATING_CONSTANTS["SCALE"]
    elif new_phi ** 2 * delta_sum < -1000.0 / RATING_CONSTANTS["SCALE"]:
        new_mu = mu - 1000.0 / RATING_CONSTANTS["SCALE"]
    else:
        new_mu = mu + new_phi ** 2 * delta_sum

    target.new_rating = (
        new_mu * RATING_CONSTANTS["SCALE"] + RATING_CONSTANTS["BASE_RATING"]
    )
    target.new_rd = min(new_phi * RATING_CONSTANTS["SCALE"], RATING_CONSTANTS["MAX_RD"])
    target.volatility = min(new_volatility, RATING_CONSTANTS["MAX_VOLATILITY"])


def extract_player_info(input_filename):
    players_dict = {}
    with open(input_filename, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            player = eval(line)
            try:
                fide_id = int(player["id"])
                name = player["name"]
                federation = player["fed"]
                sex = player.get("sex", "M")
            except ValueError:
                print(f"Error reading player data at line {line}")
                print(player)
                continue
            if "w" in player["flag"]:
                sex = "F"
            b_year = player.get("b_year").split(".")[-1]
            players_dict[fide_id] = {
                "name": name,
                "federation": federation,
                "sex": sex,
                "b_year": b_year,
            }
    return players_dict


class RatingListWriter:
    """Handles writing rating lists in various formats"""

    def __init__(self, players, players_info, year, upload_to_s3=False):
        self.players = players
        self.players_info = players_info
        self.year = year
        self.upload_to_s3 = upload_to_s3
        self.sorted_players = sorted(
            players.values(), key=lambda p: p.rating, reverse=True
        )

    def write_raw_ratings(self, filename):
        """Writes raw rating data in space-separated format"""
        lines = [
            f"{player.id} {player.rating:.7f} {player.rd:.7f} {player.volatility:.7f}\n"
            for player in self.players.values()
        ]
        with open(filename, "w") as out_file:
            out_file.writelines(lines)

    def _get_player_details(self, player):
        """Extract and normalize player details"""
        info = self.players_info.get(player.id, {})
        details = {
            "name": info.get("name", ""),
            "federation": info.get("federation", ""),
            "sex": info.get("sex", ""),
            "b_year": self._normalize_birth_year(info.get("b_year", "")),
        }
        return details

    def _normalize_birth_year(self, b_year):
        """Normalize birth year to 4-digit format"""
        if not b_year.isdigit():
            return 0
        year = int(b_year)
        if year < 100:
            year += 1900
            if self.year - year < 0:
                year += 100
        return year

    def _write_category_file(self, filepath, header, players_with_rank):
        """Generic method to write a category file"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            f.write(header + "\n")
            for rank, player, details in players_with_rank:
                # For women's and girls' categories, exclude sex from the output
                if any(cat in filepath for cat in ["women.txt", "girls.txt"]):
                    line = (
                        f"{rank} {details['name']}\t{details['federation']} "
                        f"{details['b_year']} {player.rating:.7f} "
                        f"{player.rd:.7f} {player.id}\n"
                    )
                else:
                    line = (
                        f"{rank} {details['name']}\t{details['federation']} "
                        f"{details['b_year']} {details.get('sex', '')} "
                        f"{player.rating:.7f} {player.rd:.7f} {player.id}\n"
                    )
                f.write(line)

    def write_global_lists(self, output_dir):
        """Writes global rating lists for different categories and optionally uploads to S3"""
        categories = {
            "open": {"max_count": 100, "condition": lambda p, d: True},
            "women": {"max_count": 100, "condition": lambda p, d: d["sex"] == "F"},
            "juniors": {
                "max_count": 100,
                "condition": lambda p, d: self.year - d["b_year"] <= 20,
            },
            "girls": {
                "max_count": 100,
                "condition": lambda p, d: d["sex"] == "F"
                and self.year - d["b_year"] <= 20,
            },
        }

        # Initialize DataManager if upload_to_s3 is enabled
        data_manager = None
        if hasattr(self, "upload_to_s3") and self.upload_to_s3:
            from aws.data_manager import DataManager

            data_manager = DataManager(use_s3=True)

        for category, settings in categories.items():
            qualified_players = []
            count = 0

            for player in self.sorted_players:
                if player.rd > 75:
                    continue

                details = self._get_player_details(player)
                if not details["name"]:
                    continue

                if settings["condition"](player, details):
                    count += 1
                    qualified_players.append((count, player, details))
                    if count >= settings["max_count"]:
                        break

            if qualified_players:
                filepath = os.path.join(output_dir, f"{category}.txt")
                # Different headers for women/girls vs other categories
                if category in ["women", "girls"]:
                    header = "Rank Name Federation BirthYear Rating RD"
                else:
                    header = "Rank Name Federation BirthYear Sex Rating RD"
                self._write_category_file(filepath, header, qualified_players)

                # Upload to S3 if enabled
                if data_manager:
                    try:
                        success = data_manager.save_file(filepath)
                        if success:
                            logging.info(f"Successfully uploaded {filepath} to S3")
                        else:
                            logging.error(f"Failed to upload {filepath} to S3")
                    except Exception as e:
                        logging.error(f"Error uploading {filepath} to S3: {str(e)}")


def write_federation_lists(dir, filename, players, players_info, year):
    """Writes rating lists for each federation, separated by category (open, women, juniors, girls)"""
    sorted_players = sorted(players.values(), key=lambda p: p.rating, reverse=True)
    federation_files = {}
    federation_counts = {
        fed: {"open": 0, "women": 0, "juniors": 0, "girls": 0} for fed in FEDERATIONS
    }

    def get_file_handle(federation, category):
        """Gets or creates file handle for federation/category combination"""
        key = (federation, category)
        if key not in federation_files:
            path = os.path.join(dir, federation, filename)
            os.makedirs(path, exist_ok=True)
            filepath = os.path.join(path, f"{category}.txt")
            header = "Rank Name Federation BirthYear Sex Rating RD\n"
            if category in ["women", "girls"]:  # These categories don't need Sex column
                header = "Rank Name Federation BirthYear Rating RD\n"
            with open(filepath, "w") as f:
                f.write(header)
        return os.path.join(dir, federation, filename, f"{category}.txt")

    def write_player_line(
        filepath, rank, player, player_info, b_year, include_sex=True
    ):
        """Writes a single player entry to the specified file"""
        sex = player_info.get("sex", "")
        line_parts = [
            str(rank),
            player_info.get("name", ""),
            player_info.get("federation", ""),
            str(b_year),
        ]
        if include_sex:
            line_parts.append(sex)
        line_parts.extend([f"{player.rating:.7f}", f"{player.rd:.7f}", str(player.id)])
        line = f"{line_parts[0]} {line_parts[1]}\t{' '.join(line_parts[2:])}\n"
        with open(filepath, "a") as f:
            f.write(line)

    for player in sorted_players:
        player_info = players_info.get(player.id, {})

        # Skip invalid entries
        if (
            player.rd > 75
            or not player_info.get("name")
            or len(player_info.get("federation", "")) != 3
        ):
            continue

        federation = player_info["federation"]
        b_year = player_info.get("b_year", "")

        # Normalize birth year
        if b_year.isdigit():
            b_year = int(b_year)
            if b_year < 100:
                b_year += 1900 + (100 if year - b_year < 0 else 0)
        else:
            b_year = 0

        sex = player_info.get("sex", "")
        is_junior = year - b_year <= 20 if b_year else False

        # Write to appropriate category files if count < 100
        categories = []
        if federation_counts[federation]["open"] < 100:
            categories.append(("open", True))
        if sex == "F" and federation_counts[federation]["women"] < 100:
            categories.append(("women", False))
        if is_junior:
            if federation_counts[federation]["juniors"] < 100:
                categories.append(("juniors", True))
            if sex == "F" and federation_counts[federation]["girls"] < 100:
                categories.append(("girls", False))

        for category, include_sex in categories:
            federation_counts[federation][category] += 1
            filepath = get_file_handle(federation, category)
            write_player_line(
                filepath,
                federation_counts[federation][category],
                player,
                player_info,
                b_year,
                include_sex,
            )


def apply_new_ratings(players):
    for player in players.values():
        player.rating = player.new_rating
        player.rd = player.new_rd


def main(
    ratings_filename,
    games_filename,
    output_filename,
    top_rating_list_dir,
    top_rating_list_filename,
    player_info_filename,
    year,
    upload_to_s3=False,
):

    players = {}
    players_info = {}

    print(f"Opening {ratings_filename}...")

    # Reading player ratings
    with open(ratings_filename, "r") as rating_file:
        for line in tqdm(rating_file, desc="Reading player ratings"):  # file iteration
            player_data = line.strip().split()
            player_id = int(player_data[0])
            rating = float(player_data[1])
            rd = float(player_data[2])
            volatility = float(player_data[3])
            players[player_id] = Player(player_id, rating, rd, volatility)

    print("Extracting player info...")
    players_info = extract_player_info(player_info_filename)

    # Reading games
    with open(games_filename, "r") as game_file:
        game_lines = list(game_file)  # Convert iterator to list for length and tqdm
        i = 0
        pbar = tqdm(total=len(game_lines), desc="Reading games")
        while i < len(game_lines):
            player_data = game_lines[i].strip().split()
            player_id = int(player_data[0])
            count = int(player_data[1])
            for j in range(count):
                i += 1
                try:
                    game_data = game_lines[i].strip().split()
                except IndexError:
                    print(
                        ratings_filename,
                        games_filename,
                        output_filename,
                        top_rating_list_dir,
                        top_rating_list_filename,
                        player_info_filename,
                        year,
                    )
                    print(f"Error reading game data at line {i+1}")
                    continue
                if len(game_data) == 2:
                    opponent_id = int(game_data[0])
                    score = float(game_data[1])
                    game = GameResult(opponent_id=opponent_id, score=score)
                    add_game(player_id, game, players)
                else:
                    opponent_rating = float(game_data[0])
                    opponent_rd = float(game_data[1])
                    score = float(game_data[2])
                    game = GameResult(
                        opponent_rating=opponent_rating,
                        opponent_rd=opponent_rd,
                        score=score,
                    )
                    add_game(player_id, game, players)
            pbar.update(count + 1)
            i += 1
        pbar.close()

    # Updating player ratings
    for player in tqdm(players.values(), desc="Updating player ratings"):
        glicko2_update(player, players)

    apply_new_ratings(players)

    # Create a RatingListWriter instance
    writer = RatingListWriter(players, players_info, year, upload_to_s3)

    # Write raw ratings to output file
    writer.write_raw_ratings(output_filename)
    print(f"Results written to {output_filename}")

    # Write global rating lists
    writer.write_global_lists(
        os.path.join(top_rating_list_dir, top_rating_list_filename)
    )

    # Write federation-specific lists
    write_federation_lists(
        top_rating_list_dir, top_rating_list_filename, players, players_info, year
    )
    print(f"Rating lists written to {top_rating_list_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate Glicko-2 ratings")
    parser.add_argument("ratings_file", help="Input ratings file")
    parser.add_argument("games_file", help="Input games file")
    parser.add_argument("output_file", help="Output ratings file")
    parser.add_argument("top_rating_list_dir", help="Directory for top rating lists")
    parser.add_argument(
        "top_rating_list_filename", help="Filename for top rating lists"
    )
    parser.add_argument("player_info_filename", help="Player info filename")
    parser.add_argument("year", type=int, help="Year for processing")
    parser.add_argument(
        "--upload-to-s3", action="store_true", help="Upload results to S3 bucket"
    )

    args = parser.parse_args()

    main(
        args.ratings_file,
        args.games_file,
        args.output_file,
        args.top_rating_list_dir,
        args.top_rating_list_filename,
        args.player_info_filename,
        args.year,
        args.upload_to_s3,
    )
