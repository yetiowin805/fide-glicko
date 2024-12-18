"""Configuration settings for the FIDE Glicko-2 project."""

# AWS S3 Configuration
S3_CONFIG = {
    'bucket_name': 'chess-glicko',  # Replace with your bucket name
    'region_name': 'us-east-2',         # Replace with your preferred region
    
    # S3 directory structure
    'directories': {
        'player_info': 'player_info',
        'rating_lists': 'rating_lists',
        'raw_tournament': 'raw_tournament_data',
        'clean_numerical': 'clean_numerical',
        'top_rating_lists': 'top_rating_lists'
    }
}

# Local directory structure
LOCAL_DIRS = {
    'player_info': 'data/player_info',
    'rating_lists': 'data/rating_lists',
    'raw_tournament': 'data/raw_tournament_data',
    'clean_numerical': 'data/clean_numerical',
    'top_rating_lists': 'data/top_rating_lists'
} 