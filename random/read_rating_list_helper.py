import os
import shutil
import sys

def run_reader(time_control, group, rank, start_year, start_month):

    for year in range(start_year, 2024):
        # For the start year, use the provided start month. For other years, start from January.
        start_m = start_month if year == start_year else 1
        
        # For 2023, end in September. For other years, end in December.
        end_m = 9 if year == 2023 else 12

        for month in range(start_m, end_m + 1):
            
            temp_year, temp_month = year, month

            next_month = month + 1
            next_year = year

            # Handle year-end rollover
            if next_month > 12:
                next_month = 1
                next_year += 1

            if not os.path.exists(f"./top_rating_lists/{time_control}/{next_year:04d}-{next_month:02d}"):
                break

            cmd = (f"python3 read_rating_list.py "
                   f"./top_rating_lists/{time_control}/{next_year:04d}-{next_month:02d}/{group} "
                   f"{rank}")
            
            
            os.system(cmd)

def main(time_control, group, rank):
    # Run for Standard
    run_reader(time_control, group, rank, 2011, 12)

if __name__ == "__main__":
    if len(sys.argv) != 4:  
        print(f"Usage: {sys.argv[0]} <time_control> <group> <rank>")
        sys.exit(1)
    
    time_control = sys.argv[1]
    group = sys.argv[2]
    rank = sys.argv[3]

    main(time_control, group, rank)