# FIDE Glicko-2 Rating System

This project recalculates the FIDE (International Chess Federation) rating list using the Glicko-2 rating system. It processes historical chess game data and generates updated ratings for Standard, Rapid, and Blitz time controls.

## Overview

The system processes FIDE tournament data and recalculates player ratings using the Glicko-2 algorithm, which provides more accurate rating estimates compared to the traditional Elo system by incorporating rating deviation (RD) and rating volatility.

## Features

- Processes FIDE tournament data for Standard, Rapid, and Blitz time controls
- Calculates ratings using the Glicko-2 algorithm
- Generates global and federation-specific rating lists
- Handles historical data from 2012 onwards (with special handling for 2011-12 transition)
- Supports multiple time control categories (Standard, Rapid, Blitz)

## Pipeline Structure

The data processing pipeline consists of several steps:

1. **Download Player Data** (`download_player_data.py`)
   - Downloads FIDE rating lists
   - Processes raw player information

2. **Process FIDE Rating Lists** (`process_fide_rating_list.py`)
   - Converts raw rating data into structured format
   - Handles different historical data formats

3. **Tournament Data Processing**
   - Scrapes and processes tournament data
   - Extracts game results and player information

4. **Data Cleaning** (`remove_duplicates.py`)
   - Removes duplicate game entries
   - Validates game results

5. **Rating Calculation** (`glicko2.py`)
   - Implements the Glicko-2 algorithm
   - Updates player ratings, RD, and volatility
   - Generates rating lists by category and federation

## Usage

Run the complete pipeline using:

```bash
python src/run_pipeline.py --start_month YYYY-MM --end_month YYYY-MM --data_dir /path/to/data
```

Optional arguments:
- `--download_player_data y/n`: Download and process FIDE rating lists
- `--scrape_fide y/n`: Scrape FIDE website for tournament data

## Data Directory Structure

```
data/
├── player_info/
│   ├── raw/
│   └── processed/
├── rating_lists/
│   ├── Standard/
│   ├── Rapid/
│   └── Blitz/
├── raw_tournament_data/
├── clean_numerical/
└── top_rating_lists/
```
