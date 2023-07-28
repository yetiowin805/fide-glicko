# import random

# # Define the players
# players = list(range(1001, 1011))
# results = ["1.0", "0.5", "0.0"]

# # Function to generate a game result
# def generate_result():
#     result = random.choice(results)
#     if result == "1.0":
#         return ("1.0", "0.0")
#     elif result == "0.5":
#         return ("0.5", "0.5")
#     else:
#         return ("0.0", "1.0")

# # Store results of games
# game_results = {}

# # Simulate games
# for i in range(len(players)):
#     player_results = []
#     for j in range(len(players)):
#         if i != j:
#             if (players[j], players[i]) in game_results:
#                 result = game_results[(players[j], players[i])]
#                 player_results.append((players[j], result[1]))
#             else:
#                 result = generate_result()
#                 game_results[(players[i], players[j])] = result
#                 player_results.append((players[j], result[0]))
#     game_results[players[i]] = player_results

# # Write results to file
# with open("test.txt", "w") as f:
#     for player, games in game_results.items():
#         if isinstance(games, list):  # Ensure we're looking at the game list, not an individual result
#             f.write(f"{player} 9\n")
#             for game in games:
#                 f.write(f"{game[0]} {game[1]}\n")

# Define the initial values
initial_rating = 1500
rating_deviation = 350
volatility = 0.06

# Define the list of players
players = list(range(1001, 1011))

# Create the file with the ratings
with open("ratings.txt", "w") as f:
    for player in players:
        f.write(f"{player} {initial_rating} {rating_deviation} {volatility}\n")