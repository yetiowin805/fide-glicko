import os
from collections import defaultdict
import threading
import argparse

# Sixth command in pipeline

class Player:
    def __init__(self):
        self.num_opponents = 0
        self.opponents_fide = []
        self.results = []
        self.opponents_ratings = []
        self.opponents_rds = []
        self.scores = []

def display_progress_bar(total_files, processed_files):
    lock = threading.Lock()
    lock.acquire()
    bar_width = 50
    progress = processed_files / total_files
    pos = int(bar_width * progress)
    bar = '[' + '=' * pos + '>' + ' ' * (bar_width - pos - 1) + ']'
    print(f'\r{bar} {int(progress * 100)}%', end='', flush=True)
    lock.release()

def count_files_in_directory(dir_path):
    total_files = 0
    for root, dirs, files in os.walk(dir_path):
        total_files += len(files)
    return total_files

def process_file(path, local_players_map):
    print(f"Processing {path}")
    with open(path, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                break
            parts = line.strip().split()
            fide_id, n = int(parts[0]), int(parts[1])
            player = local_players_map.setdefault(fide_id, Player())
            for _ in range(n):
                line = file.readline()
                parts = line.strip().split()
                if len(parts) == 2:
                    player.opponents_fide.append(int(parts[0]))
                    player.results.append(float(parts[1]))
                else:
                    player.opponents_ratings.append(float(parts[0]))
                    player.opponents_rds.append(float(parts[1]))
                    player.scores.append(float(parts[2]))

    with open(path, 'w') as file:
        for fide_id, player in local_players_map.items():
            file.write(f"{fide_id} {len(player.opponents_fide) + len(player.opponents_ratings)}\n")
            for opponent_fide, result in zip(player.opponents_fide, player.results):
                file.write(f"{opponent_fide} {result:.1f}\n")
            for rating, rd, score in zip(player.opponents_ratings, player.opponents_rds, player.scores):
                file.write(f"{rating} {rd} {score}\n")

def process_directory(dir_path, total_files, processed_files=[0]):
    for entry in os.listdir(dir_path):
        full_path = os.path.join(dir_path, entry)
        if os.path.isfile(full_path):
            local_players_map = defaultdict(Player)
            process_file(full_path, local_players_map)
            processed_files[0] += 1
            display_progress_bar(total_files, processed_files[0])
        elif os.path.isdir(full_path):
            process_directory(full_path, total_files, processed_files)

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Combines duplicate FIDE IDs into a single entry.')
    parser.add_argument('--root_dir', type=str, help='Root directory to process', required=True)

    # Parse arguments
    args = parser.parse_args()

    root_dir = args.root_dir
    
    total_files = count_files_in_directory(root_dir)
    process_directory(root_dir, total_files)
    print("\nAll files processed and updated!")
