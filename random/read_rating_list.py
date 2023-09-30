import sys
from tqdm import tqdm
import math
import os

def main(ratings_filename, rank):
    with open(ratings_filename, 'r') as file:
        # Skip the header line
        next(file)
        
        for line in file:
            # Split the line into components
            components = line.split()
            
            # Extract rank, name, and rating from the components
            current_rank = components[0]
            name = ' '.join(components[1:-6])
            rating = float(components[-3])
            
            # Check if the current rank matches the input rank
            if current_rank == rank:
                print(f"{name}\t{rating}")
                #print(name)
                #print(rating)
                break


if __name__ == "__main__":
    if len(sys.argv) != 3:  
        print(f"Usage: {sys.argv[0]} <ratings_file> <rank>")
        sys.exit(1)
    
    ratings_filename = sys.argv[1]
    rank = sys.argv[2]

    main(ratings_filename, rank)