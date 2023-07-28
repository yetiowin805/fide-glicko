import os
import shutil

def run_glicko(folder, start_year, start_month):
    for year in range(start_year, 2024):
        # For the start year, use the provided start month. For other years, start from January.
        start_m = start_month if year == start_year else 1
        
        # For 2023, end in June. For other years, end in December.
        end_m = 6 if year == 2023 else 12

        for month in range(start_m, end_m + 1):
            next_month = month + 1
            next_year = year

            # Handle year-end rollover
            if next_month > 12:
                next_month = 1
                next_year += 1

            cmd = (f"python glicko2.py rating_lists/{folder}/{year:04d}-{month:02d}.txt "
                   f"clean_numerical/{year:04d}-{month:02d}/{folder}/games.txt "
                   f"./rating_lists/{folder}/{next_year:04d}-{next_month:02d}.txt "
                   f"./rating_lists_with_info/{folder}/{next_year:04d}-{next_month:02d}.txt")
            
            os.system(cmd)

def main():
    # Run for Standard
    run_glicko("Standard", 2007, 10)
    
    # Copy the 2011-12 ratings to Rapid and Blitz folders
    src_file = "./rating_lists/Standard/2011-12.txt"
    shutil.copy(src_file, "./rating_lists/Rapid/2011-12.txt")
    shutil.copy(src_file, "./rating_lists/Blitz/2011-12.txt")

    # Run for Rapid and Blitz starting from 2011-12
    run_glicko("Rapid", 2011, 12)
    run_glicko("Blitz", 2011, 12)

if __name__ == "__main__":
    main()