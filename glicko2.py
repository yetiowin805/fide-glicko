import sys
from tqdm import tqdm
import math
import os

BASE_RATING = 1500.0
BASE_RD = 350.0
BASE_VOLATILITY = 0.09
TAU = 0.2

# Pre-computed constants
PI_SQUARED = math.pi**2
SCALE = 173.7178

class GameResult:
    def __init__(self, opponent_id, score):
        self.opponent_id = opponent_id
        self.score = score

class Player:
    def __init__(self, player_id, rating=BASE_RATING, rd=BASE_RD, volatility=BASE_VOLATILITY):
        self.id = player_id
        self.rating = rating
        self.rd = rd
        self.volatility = volatility
        self.new_rating = rating
        self.new_rd = rd
        self.games = []

def add_game(player_id, game, players_dict):
    player = players_dict.setdefault(player_id, Player(player_id))
    opponent = players_dict.setdefault(game.opponent_id, Player(game.opponent_id))
    player.games.append(game)

def f(x, delta, v, A):
    ex = math.exp(x)
    ex_v_sum = v + ex
    return (ex * (delta**2 - v - ex)) / (2 * ex_v_sum**2) - (x - A) / TAU**2

def glicko2_update(target, players):
    if len(target.games) == 0:
        phi = target.rd / SCALE
        phi_star = math.sqrt(phi**2 + target.volatility**2)
        target.new_rd = phi_star * SCALE
        if target.new_rd > 500:
            target.new_rd = 500
        return

    mu = (target.rating - 1500.0) / SCALE
    phi = target.rd / SCALE

    v_inv = 0
    delta_sum = 0

    for game in target.games:
        opponent = players[game.opponent_id]
        mu_j = (opponent.rating - 1500.0) / SCALE
        phi_j = opponent.rd / SCALE

        g_phi_j = 1.0 / math.sqrt(1.0 + (3.0 * phi_j**2) / PI_SQUARED)
        
        e_val = 1.0 / (1.0 + math.exp(-g_phi_j * (mu - mu_j)))

        v_inv += g_phi_j**2 * e_val * (1 - e_val)
        delta_sum += g_phi_j * (game.score - e_val)

    v = 1.0 / v_inv

    delta = v * delta_sum

    a = math.log(target.volatility**2)
    A = a
    if delta**2 > phi**2 + v:
        B = math.log(delta**2 - phi**2 - v)
    else:
        k = 1
        while f(a - k * TAU, delta, v, a) < 0:
            k += 1
        B = a - k * TAU

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
    phi_star = math.sqrt(phi**2 + new_volatility**2)
    new_phi = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
    if new_phi**2 * delta_sum > 700.0 / SCALE:
        new_mu = mu + 700.0 / SCALE
    elif new_phi**2 * delta_sum < -700.0 / SCALE:
        new_mu = mu - 700.0 / SCALE
    else:
        new_mu = mu + new_phi**2 * delta_sum

    target.new_rating = new_mu * SCALE + 1500.0
    target.new_rd = new_phi * SCALE
    target.volatility = new_volatility

    if target.new_rating < 0:
        target.new_rating = 0
    elif target.new_rating > 4000:
        target.new_rating = 4000
    if target.new_rd > 500:
        target.new_rd = 500
    if target.volatility > 0.1:
        target.volatility = 0.1

def extract_player_info(input_filename):
    with open(input_filename, 'r') as f:
        lines = f.readlines()

    # Skip the header line
    lines = lines[1:]

    players_dict = {}

    for line in lines:
        # Extract relevant fields
        fide_id = int(line[:9].strip())
        name = line[9:64].strip()
        federation = line[76:79].strip()
        sex = line[80:81].strip()
        b_year = line[-11:-7].strip()

        # Store in a dictionary
        players_dict[fide_id] = {
            "name": name,
            "federation": federation,
            "sex": sex,
            "b_year": b_year
        }

    return players_dict

def write_to_file(filename, players):
    lines = [
        f"{player.id} {player.rating:.6f} {player.rd:.6f} {player.volatility:.6f}\n"
        for player in players.values()
    ]
    
    with open(filename, 'w') as out_file:
        out_file.writelines(lines)

def write_to_pretty_file(filename, players, players_info, top_filename = None):
    # Sort players by rating in descending order
    sorted_players = sorted(players.values(), key=lambda p: p.rating, reverse=True)

    with open(filename, 'w') as out_file:
        # Write headers first
        out_file.write("ID Name Federation BirthYear Sex Rating RD Volatility\n")
        
        for player in sorted_players:
            player_info = players_info.get(player.id, {})
            name = player_info.get('name', '')
            federation = player_info.get('federation', '')
            b_year = player_info.get('b_year', '')
            sex = player_info.get('sex', '')
            
            line = f"{player.id} {name} {federation} {b_year} {sex} {player.rating:.6f} {player.rd:.6f} {player.volatility:.6f}\n"
            out_file.write(line)
    if top_filename is not None:
        os.makedirs(top_filename, exist_ok=True)
        with open(os.path.join(top_filename, "open.txt"), 'w') as out_file:
            # Write headers first
            out_file.write("Rank Name Federation BirthYear Sex Rating RD\n")

            counter = 1
            
            for player in sorted_players:
                if counter > 100:
                    break
                player_info = players_info.get(player.id, {})
                if player.rd < 85:
                    name = player_info.get('name', '')
                    federation = player_info.get('federation', '')
                    b_year = player_info.get('b_year', '')
                    sex = player_info.get('sex', '')
                    line = f"{counter} {name}\t{federation} {b_year} {sex} {player.rating:.2f} {player.rd:.2f}\n"
                    out_file.write(line)
                    counter += 1
        with open(os.path.join(top_filename, "women.txt"), 'w') as out_file:
            # Write headers first
            out_file.write("Rank Name Federation BirthYear Rating RD\n")

            counter = 1
            
            for player in sorted_players:
                if counter > 100:
                    break
                player_info = players_info.get(player.id, {})
                sex = player_info.get('sex', '')
                if sex == "F" and player.rd < 75:
                    name = player_info.get('name', '')
                    federation = player_info.get('federation', '')
                    b_year = player_info.get('b_year', '')
                    line = f"{counter} {name}\t{federation} {b_year} {player.rating:.2f} {player.rd:.2f}\n"
                    out_file.write(line)
                    counter += 1
        with open(os.path.join(top_filename, "juniors.txt"), 'w') as out_file:
            # Write headers first
            out_file.write("Rank Name Federation BirthYear Sex Rating RD\n")

            counter = 1
            
            for player in sorted_players:
                if counter > 100:
                    break
                player_info = players_info.get(player.id, {})
                b_year = player_info.get('b_year', '')
                if b_year.isdigit() and int(b_year) >= 2003 and player.rd < 75:
                    name = player_info.get('name', '')
                    federation = player_info.get('federation', '')
                    sex = player_info.get('sex', '')
                    line = f"{counter} {name}\t{federation} {b_year} {sex} {player.rating:.2f} {player.rd:.2f}\n"
                    out_file.write(line)
                    counter += 1
        with open(os.path.join(top_filename, "girls.txt"), 'w') as out_file:
            # Write headers first
            out_file.write("Rank Name Federation BirthYear Rating RD\n")

            counter = 1
            
            for player in sorted_players:
                if counter > 100:
                    break
                player_info = players_info.get(player.id, {})
                b_year = player_info.get('b_year', '')
                sex = player_info.get('sex', '')
                if sex == 'F' and b_year.isdigit() and int(b_year) >= 2003 and player.rd < 75:
                    name = player_info.get('name', '')
                    federation = player_info.get('federation', '')
                    line = f"{counter} {name}\t{federation} {b_year} {player.rating:.2f} {player.rd:.2f}\n"
                    out_file.write(line)
                    counter += 1

def apply_new_ratings(players):
    for player in players.values():
        player.rating = player.new_rating
        player.rd = player.new_rd

def main(ratings_filename, games_filename, output_filename, pretty_output_filename, top_rating_list_filename):

    players = {}
    players_info = {}

    print(f"Opening {ratings_filename}...")

    # Reading player ratings
    with open(ratings_filename, 'r') as rating_file:
        for line in tqdm(rating_file, desc="Reading player ratings"):  # file iteration
            player_data = line.strip().split()
            player_id = int(player_data[0])
            rating = float(player_data[1])
            rd = float(player_data[2])
            volatility = float(player_data[3])
            players[player_id] = Player(player_id, rating, rd, volatility)

    print("Extracting player info...")
    players_info = extract_player_info("players_list_foa.txt")

    # Reading games
    with open(games_filename, 'r') as game_file:
        game_lines = list(game_file)  # Convert iterator to list for length and tqdm
        i = 0
        pbar = tqdm(total=len(game_lines), desc="Reading games")
        while i < len(game_lines):
            player_data = game_lines[i].strip().split()
            player_id = int(player_data[0])
            count = int(player_data[1])
            for j in range(count):
                i += 1
                game_data = game_lines[i].strip().split()
                opponent_id = int(game_data[0])
                score = float(game_data[1])
                game = GameResult(opponent_id, score)
                add_game(player_id, game, players)
            pbar.update(count+1)
            i += 1
        pbar.close()

    # Updating player ratings
    for player in tqdm(players.values(), desc="Updating player ratings"):
        glicko2_update(player, players)

    apply_new_ratings(players)

    # Write updated ratings to the output file
    write_to_file(output_filename, players)
    write_to_pretty_file(pretty_output_filename, players, players_info, top_rating_list_filename)
    print(f"Results written to {output_filename}")

if __name__ == "__main__":
    # At least 4 and at most 5 user-provided arguments
    if len(sys.argv) < 5 or len(sys.argv) > 6:  
        print(f"Usage: {sys.argv[0]} <ratings_file> <games_file> <output_file> <pretty_output_file> [optional: top_rating_list_filename]")
        sys.exit(1)
    
    ratings_filename = sys.argv[1]
    games_filename = sys.argv[2]
    output_filename = sys.argv[3]
    pretty_output_filename = sys.argv[4]
    
    # Check if top_rating_list is provided
    top_rating_list_filename = sys.argv[5] if len(sys.argv) == 6 else None  # or provide a default value instead of None

    main(ratings_filename, games_filename, output_filename, pretty_output_filename, top_rating_list_filename)