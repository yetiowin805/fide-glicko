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

FEDERATIONS = [
        'AFG', 'ALB', 'ALG', 'AND', 'ANG', 'ANT', 'ARG', 'ARM', 'ARU', 'AUS', 
        'AUT', 'AZE', 'BAH', 'BRN', 'BAN', 'BAR', 'BLR', 'BEL', 'BIZ', 'BER', 
        'BHU', 'BOL', 'BIH', 'BOT', 'BRA', 'IVB', 'BRU', 'BUL', 'BUR', 'BDI', 
        'CAM', 'CMR', 'CAN', 'CPV', 'CAY', 'CAF', 'CHA', 'CHI', 'CHN', 'TPE', 
        'COL', 'COM', 'CGO', 'CRC', 'CRO', 'CUB', 'CYP', 'CZE', 'COD', 'DEN', 
        'DJI', 'DMA', 'DOM', 'ECU', 'EGY', 'ESA', 'ENG', 'GEQ', 'ERI', 'EST', 
        'SWZ', 'ETH', 'FAI', 'FIJ', 'FIN', 'FRA', 'GAB', 'GAM', 'GEO', 'GER', 
        'GHA', 'GRE', 'GRN', 'GUM', 'GUA', 'GCI', 'GUY', 'HAI', 'HON', 'HKG', 
        'HUN', 'ISL', 'IND', 'INA', 'IRI', 'IRQ', 'IRL', 'IOM', 'ISR', 'ITA', 
        'CIV', 'JAM', 'JPN', 'JCI', 'JOR', 'KAZ', 'KEN', 'KOS', 'KUW', 'KGZ', 
        'LAO', 'LAT', 'LBN', 'LES', 'LBR', 'LBA', 'LIE', 'LTU', 'LUX', 'MAC', 
        'MAD', 'MAW', 'MAS', 'MDV', 'MLI', 'MLT', 'MTN', 'MRI', 'MEX', 'MDA', 
        'MNC', 'MGL', 'MNE', 'MAR', 'MOZ', 'MYA', 'NAM', 'NRU', 'NEP', 'NED', 
        'AHO', 'NZL', 'NCA', 'NIG', 'NGR', 'MKD', 'NOR', 'OMA', 'PAK', 'PLW', 
        'PLE', 'PAN', 'PNG', 'PAR', 'PER', 'PHI', 'POL', 'POR', 'PUR', 'QAT', 
        'ROU', 'RUS', 'RWA', 'SKN', 'LCA', 'SMR', 'STP', 'KSA', 'SCO', 'SEN', 
        'SRB', 'SEY', 'SLE', 'SGP', 'SVK', 'SLO', 'SOL', 'SOM', 'RSA', 'KOR', 
        'SSD', 'ESP', 'SRI', 'VIN', 'SUD', 'SUR', 'SWE', 'SUI', 'SYR', 'TJK', 
        'TAN', 'THA', 'TLS', 'TOG', 'TTO', 'TUN', 'TUR', 'TKM', 'UGA', 'UKR', 
        'UAE', 'USA', 'URU', 'ISV', 'UZB', 'VEN', 'VIE', 'WLS', 'YEM', 'ZAM', 
        'ZIM', 'FID', 'SIN', 'TRI', 'LIB'
    ]

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
    with open(input_filename, 'r', encoding='utf-8', errors='replace') as f:
        header = f.readline().strip()  # Read the header line
        lines = f.readlines()          # Read the rest of the lines

    # Determine column positions based on the header
    id_start = header.index("ID")
    name_start = header.index("Name")
    name_end = min(header.index("Tit"),header.index("Fed"))
    fed_start = header.index("Fed")
    if "Born" in header:
        bday_start = header.index("Born")
    elif "B-day" in header:
        bday_start = header.index("B-day")
    flag_start = header.index("Flag")

    players_dict = {}

    for line in lines:
        # Extract relevant fields
        try:
            fide_id = int(line[id_start:name_start].strip())
        except ValueError:
            continue
        name = line[name_start:name_end].strip()
        federation = line[fed_start:fed_start+3].strip()
        sex = line[flag_start:].strip()
        if 'w' in sex:
            sex = 'F'
        else:
            sex = 'M'
        b_year = line[bday_start:bday_start+4].strip()

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
        f"{player.id} {player.rating:.7f} {player.rd:.7f} {player.volatility:.7f}\n"
        for player in players.values()
    ]
    
    with open(filename, 'w') as out_file:
        out_file.writelines(lines)

def write_to_pretty_file(filename, players, players_info, year):
    # Sort players by rating in descending order
    sorted_players = sorted(players.values(), key=lambda p: p.rating, reverse=True)

    count = 0
    count_women = 0
    count_juniors = 0
    count_girls = 0

    os.makedirs(filename, exist_ok=True)

    for player in sorted_players:
        player_info = players_info.get(player.id, {})
        if player.rd > 75:
            continue
        name = player_info.get('name', '')
        if name == '':
                continue
        federation = player_info.get('federation', '')
        b_year = player_info.get('b_year', '')
        sex = player_info.get('sex', '')
        if count == 0:
            os.makedirs(os.path.join(filename), exist_ok=True)
            f = open(os.path.join(filename, "open.txt"), 'w')
            f.write("Rank Name Federation BirthYear Sex Rating RD\n")
            f.close()
        if count < 100:
            count += 1
            line = f"{count} {name}\t{federation} {b_year} {sex} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
            f = open(os.path.join(filename, "open.txt"), 'a')
            f.write(line)
            f.close()
        if b_year.isdigit() and year - int(b_year) <= 20:
            if count_juniors == 0:
                f = open(os.path.join(filename, "juniors.txt"), 'w')
                f.write("Rank Name Federation BirthYear Sex Rating RD\n")
                f.close()
            if count_juniors < 100:
                count_juniors += 1
                line = f"{count_juniors} {name}\t{federation} {b_year} {sex} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                f = open(os.path.join(filename, "juniors.txt"), 'a')
                f.write(line)
                f.close()
        if sex == 'F':
            if count_women == 0:
                f = open(os.path.join(filename, "women.txt"), 'w')
                f.write("Rank Name Federation BirthYear Rating RD\n")
                f.close()
            if count_women < 100:
                count_women += 1
                line = f"{count_women} {name}\t{federation} {b_year} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                f = open(os.path.join(filename, "women.txt"), 'a')
                f.write(line)
                f.close()
            if b_year.isdigit() and year - int(b_year) <= 20:
                if count_girls == 0:
                    f = open(os.path.join(filename, "girls.txt"), 'w')
                    f.write("Rank Name Federation BirthYear Rating RD\n")
                    f.close()
                if count_girls < 100:
                    count_girls += 1
                    line = f"{count_girls} {name}\t{federation} {b_year} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                    f = open(os.path.join(filename, "girls.txt"), 'a')
                    f.write(line)
                    f.close()
                    if count_girls == 100:
                        break

def write_to_pretty_file_FED(dir, filename, players, players_info, year):
    # Sort players by rating in descending order
    sorted_players = sorted(players.values(), key=lambda p: p.rating, reverse=True)
    federation_counts = {x:0 for x in FEDERATIONS}
    federation_counts_women = {x:0 for x in FEDERATIONS}
    federation_counts_juniors = {x:0 for x in FEDERATIONS}
    federation_counts_girls = {x:0 for x in FEDERATIONS}

    for player in sorted_players:
        player_info = players_info.get(player.id, {})
        if player.rd > 75:
            continue
        name = player_info.get('name', '')
        if name == '':
                continue
        federation = player_info.get('federation', '')
        if federation_counts_girls[federation] == 100:
            continue
        b_year = player_info.get('b_year', '')
        sex = player_info.get('sex', '')
        if federation_counts[federation] == 0:
            os.makedirs(os.path.join(dir,federation,filename), exist_ok=True)
            f = open(os.path.join(dir, federation, filename, "open.txt"), 'w')
            f.write("Rank Name Federation BirthYear Sex Rating RD\n")
            f.close()
        if federation_counts[federation] < 100:
            federation_counts[federation] += 1
            line = f"{federation_counts[federation]} {name}\t{federation} {b_year} {sex} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
            f = open(os.path.join(dir, federation, filename, "open.txt"), 'a')
            f.write(line)
            f.close()
        if b_year.isdigit() and year - int(b_year) <= 20:
            if federation_counts_juniors[federation] == 0:
                f = open(os.path.join(dir, federation, filename, "juniors.txt"), 'w')
                f.write("Rank Name Federation BirthYear Sex Rating RD\n")
                f.close()
            if federation_counts_juniors[federation] < 100:
                federation_counts_juniors[federation] += 1
                line = f"{federation_counts_juniors[federation]} {name}\t{federation} {b_year} {sex} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                f = open(os.path.join(dir, federation, filename, "juniors.txt"), 'a')
                f.write(line)
                f.close()
        if sex == 'F':
            if federation_counts_women[federation] == 0:
                f = open(os.path.join(dir, federation, filename, "women.txt"), 'w')
                f.write("Rank Name Federation BirthYear Rating RD\n")
                f.close()
            if federation_counts_women[federation] < 100:
                federation_counts_women[federation] += 1
                line = f"{federation_counts_women[federation]} {name}\t{federation} {b_year} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                f = open(os.path.join(dir, federation, filename, "women.txt"), 'a')
                f.write(line)
                f.close()
            if b_year.isdigit() and year - int(b_year) <= 20:
                if federation_counts_girls[federation] == 0:
                    f = open(os.path.join(dir, federation, filename, "girls.txt"), 'w')
                    f.write("Rank Name Federation BirthYear Rating RD\n")
                    f.close()
                if federation_counts_girls[federation] < 100:
                    federation_counts_girls[federation] += 1
                    line = f"{federation_counts_girls[federation]} {name}\t{federation} {b_year} {player.rating:.7f} {player.rd:.7f} {player.id}\n"
                    f = open(os.path.join(dir, federation, filename, "girls.txt"), 'a')
                    f.write(line)
                    f.close()

def apply_new_ratings(players):
    for player in players.values():
        player.rating = player.new_rating
        player.rd = player.new_rd

def main(ratings_filename, games_filename, output_filename, top_rating_list_dir, top_rating_list_filename, player_info_filename, year):

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
    players_info = extract_player_info(player_info_filename)

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
    print(f"Results written to {output_filename}")
    write_to_pretty_file(top_rating_list_dir+top_rating_list_filename, players, players_info, year)
    write_to_pretty_file_FED(top_rating_list_dir, top_rating_list_filename, players, players_info, year)
    print(f"Rating lists written to {top_rating_list_dir}")

if __name__ == "__main__":
    if len(sys.argv) != 8:  
        print(f"Usage: {sys.argv[0]} <ratings_file> <games_file> <output_file> <top_rating_list_dir> <top_rating_list_filename> <player_info_filename> <year>")
        sys.exit(1)
    
    ratings_filename = sys.argv[1]
    games_filename = sys.argv[2]
    output_filename = sys.argv[3]
    top_rating_list_dir = sys.argv[4]
    top_rating_list_filename = sys.argv[5]
    player_info_filename = sys.argv[6]
    year = int(sys.argv[7])

    main(ratings_filename, games_filename, output_filename, top_rating_list_dir, top_rating_list_filename, player_info_filename, year)