#include "glicko2.h"

#include <parquet/arrow/reader.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>
#include <arrow/result.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <iostream>
#include <vector>
#include <utility>
#include <cmath>
#include <stdexcept>
#include <mutex>
#include <atomic>

// Include OpenMP header
#ifdef _OPENMP
#include <omp.h>
#endif

// Helper function to get value from a ChunkedArray
template <typename T>
typename T::c_type get_value(const std::shared_ptr<arrow::ChunkedArray>& array, int64_t index) {
    int chunk_index = 0;
    int64_t chunk_offset = index;
    for (const auto& chunk : array->chunks()) {
        if (chunk_offset < chunk->length()) {
            auto typed_array = std::static_pointer_cast<arrow::NumericArray<T>>(chunk);
            return typed_array->Value(chunk_offset);
        }
        chunk_offset -= chunk->length();
        ++chunk_index;
    }
    throw std::out_of_range("Index out of range in ChunkedArray");
}

void Glicko2::set_player_ratings(const std::string& ratings_file) {
    // Open the Parquet file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    auto maybe_file = arrow::io::ReadableFile::Open(ratings_file);
    if (!maybe_file.ok()) {
        throw std::runtime_error("Failed to open file: " + maybe_file.status().ToString());
    }
    infile = *maybe_file;

    // Create a Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    arrow::Status status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_reader);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create Parquet file reader: " + status.ToString());
    }

    // Read the entire file into a table
    std::shared_ptr<arrow::Table> table;
    status = parquet_reader->ReadTable(&table);
    if (!status.ok()) {
        throw std::runtime_error("Failed to read Parquet file into table: " + status.ToString());
    }

    // Check that required columns exist
    std::vector<std::string> required_columns = {"fide_id", "rating", "deviation", "volatility", "last_month_played"};
    for (const auto& col : required_columns) {
        if (!table->GetColumnByName(col)) {
            throw std::runtime_error("Missing required column: " + col);
        }
    }

    // Extract columns
    std::shared_ptr<arrow::ChunkedArray> fide_id_array = table->GetColumnByName("fide_id");
    std::shared_ptr<arrow::ChunkedArray> rating_array = table->GetColumnByName("rating");
    std::shared_ptr<arrow::ChunkedArray> deviation_array = table->GetColumnByName("deviation");
    std::shared_ptr<arrow::ChunkedArray> volatility_array = table->GetColumnByName("volatility");
    std::shared_ptr<arrow::ChunkedArray> last_month_played_array = table->GetColumnByName("last_month_played");

    // Ensure all columns have the same length
    int64_t num_rows = table->num_rows();
    if (fide_id_array->length() != num_rows ||
        rating_array->length() != num_rows ||
        deviation_array->length() != num_rows ||
        volatility_array->length() != num_rows ||
        last_month_played_array->length() != num_rows) {
        throw std::runtime_error("Column lengths do not match");
    }

    // Iterate over each row and populate the players map
    for (int64_t i = 0; i < num_rows; ++i) {
        // Extract fide_id
        int fide_id;
        if (fide_id_array->type()->id() == arrow::Type::INT64) {
            fide_id = static_cast<int>(get_value<arrow::Int64Type>(fide_id_array, i));
        } else {
            throw std::runtime_error("Unsupported fide_id type");
        }

        // Extract rating
        double rating;
        if (rating_array->type()->id() == arrow::Type::DOUBLE) {
            rating = get_value<arrow::DoubleType>(rating_array, i);
        } else {
            throw std::runtime_error("Unsupported rating type");
        }

        // Extract deviation
        double deviation;
        if (deviation_array->type()->id() == arrow::Type::DOUBLE) {
            deviation = get_value<arrow::DoubleType>(deviation_array, i);
        } else {
            throw std::runtime_error("Unsupported deviation type");
        }

        // Extract volatility
        double volatility;
        if (volatility_array->type()->id() == arrow::Type::DOUBLE) {
            volatility = get_value<arrow::DoubleType>(volatility_array, i);
        } else {
            throw std::runtime_error("Unsupported volatility type");
        }

        // Extract last_month_played
        int last_month_played;
        if (last_month_played_array->type()->id() == arrow::Type::INT32) {
            last_month_played = get_value<arrow::Int32Type>(last_month_played_array, i);
        } else {
            throw std::runtime_error("Unsupported last_month_played type");
        }

        // Add to the players map
        players[fide_id] = Player{rating, deviation, volatility, last_month_played};
    }
}

std::map<int, std::vector<std::pair<int, double>>> Glicko2::get_player_games(const std::string &games_file) {


    // 1. Open the Parquet games file
    std::shared_ptr<arrow::io::ReadableFile> infile;
    auto maybe_file = arrow::io::ReadableFile::Open(games_file);
    if (!maybe_file.ok()) {
        throw std::runtime_error("Failed to open games file: " + maybe_file.status().ToString());
    }
    infile = *maybe_file;

    // 2. Create a Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    arrow::Status status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_reader);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create Parquet file reader: " + status.ToString());
    }

    // 3. Read the entire file into a table
    std::shared_ptr<arrow::Table> table;
    status = parquet_reader->ReadTable(&table);
    if (!status.ok()) {
        throw std::runtime_error("Failed to read Parquet file into table: " + status.ToString());
    }

    // 4. Check that required columns exist
    std::vector<std::string> required_columns = {"player_1", "player_2", "outcome"};
    for (const auto& col : required_columns) {
        if (!table->GetColumnByName(col)) {
            throw std::runtime_error("Missing required column in games file: " + col);
        }
    }

    // 5. Extract columns
    std::shared_ptr<arrow::ChunkedArray> player1_array = table->GetColumnByName("player_1");
    std::shared_ptr<arrow::ChunkedArray> player2_array = table->GetColumnByName("player_2");
    std::shared_ptr<arrow::ChunkedArray> outcome_array = table->GetColumnByName("outcome");

    // 6. Ensure all columns have the same length
    int64_t num_rows = table->num_rows();
    if (player1_array->length() != num_rows ||
        player2_array->length() != num_rows ||
        outcome_array->length() != num_rows) {
        throw std::runtime_error("Column lengths do not match in games file");
    }

    // 7. Build per-player game lists
    // Using a map: player_id -> vector of (opponent_id, outcome)
    // To facilitate parallel processing, we'll first accumulate all games
    // Then process the map in parallel
    std::map<int, std::vector<std::pair<int, double>>> player_games;

    // Iterate through each row and accumulate games
    for (int64_t i = 0; i < num_rows; ++i) {

        // Extract player_1
        int player1;
        if (player1_array->type()->id() == arrow::Type::INT64) {
            player1 = static_cast<int>(get_value<arrow::Int64Type>(player1_array, i));
        } else {
            throw std::runtime_error("Unsupported player_1 type in games file");
        }

        // Extract player_2
        int player2;
        if (player2_array->type()->id() == arrow::Type::INT32) {
            player2 = get_value<arrow::Int32Type>(player2_array, i);
        } else if (player2_array->type()->id() == arrow::Type::INT64) {
            player2 = static_cast<int>(get_value<arrow::Int64Type>(player2_array, i));
        } else {
            throw std::runtime_error("Unsupported player_2 type in games file");
        }

        // Extract outcome (from player_1's perspective)
        double outcome;
        if (outcome_array->type()->id() == arrow::Type::FLOAT) {
            outcome = static_cast<double>(get_value<arrow::FloatType>(outcome_array, i));
        } else {
            throw std::runtime_error("Unsupported outcome type in games file");
        }

        // Accumulate for player_1
        player_games[player1].emplace_back(player2, outcome);

        // Accumulate for player_2 with flipped outcome
        player_games[player2].emplace_back(player1, 1.0 - outcome);
    }

    return player_games;
}

std::vector<int> Glicko2::get_player_ids(const std::map<int, std::vector<std::pair<int, double>>>& player_games) {
    std::vector<int> player_ids;
    for (const auto& [player_id, _] : player_games) {
        player_ids.push_back(player_id);
    }
    return player_ids;
}

void Glicko2::update_player_ratings_and_deviations(const std::vector<int>& player_ids, int month) {
    
    for (const auto& player_id : player_ids) {
        auto& player = players[player_id];
        player.rating = (player.rating - 1500.0) / 173.7178;
        player.deviation = std::sqrt(std::pow(player.deviation / 173.7178, 2) + (month - player.last_month_played - 1) * player.volatility * player.volatility);
        player.last_month_played = month;
    }
}

double Glicko2::f(double x, double a, double deviation, double v, double delta) const {
    return std::exp(x) * (delta * delta - deviation * deviation - v - std::exp(x)) /
    (2 * std::pow(deviation * deviation + v + std::exp(x), 2)) - (x - a) / (tau * tau);
}

double Glicko2::get_new_volatility(double deviation, double volatility, double v, double delta) const {
    double A = 2 * std::log(volatility);
    double B = std::log(delta * delta - deviation * deviation - v);
    if (B <= 0.0) {
        int k = 1;
        while (f(A - k * tau, A, deviation, v, delta) < 0.0) {
            ++k;
        }
        B = A - k * tau;
    }
    double f_A = f(A, A, deviation, v, delta);
    double f_B = f(B, A, deviation, v, delta);
    while (std::abs(B - A) > epsilon) {
        double C = A + (A - B) * f_A / (f_B - f_A);
        double f_C = f(C, A, deviation, v, delta);
        if (f_C < 0.0) {
            B = C;
            f_B = f_C;
        } else {
            A = C;
            f_A = f_C;
        }
    }
    return std::exp(A/2);

}

double binary_cross_entropy(double expected_score, double outcome) {
    return -outcome * std::log(expected_score) - (1.0 - outcome) * std::log(1.0 - expected_score);
}

double Glicko2::update_player_ratings(const std::map<int, std::vector<std::pair<int, double>>>& player_games, const std::vector<int>& player_ids, int month, int num_threads) {

    std::pair<int, Player> updated_players[player_ids.size()];
    double loss = 0.0;
    for (const auto& player_id : player_ids) {
        auto& player = players[player_id];

        double v_sum = 0.0, g_sum = 0.0;
        for (const auto& [opponent_id, outcome] : player_games.at(player_id)) {
            auto opponent_it = players.find(opponent_id);
            double opponent_rating = DEFAULT_RATING;
            double opponent_deviation = starting_deviation;

            double g = 1.0 / std::sqrt(1.0 + 3.0 * std::pow(opponent_deviation / 173.7178 / M_PI, 2));
            double expected_score = 1.0 / (1.0 + std::exp(-g * (player.rating - opponent_rating) * 173.7178 / 400.0));
            v_sum += std::pow(g, 2) * expected_score * (1.0 - expected_score);
            g_sum += g * (outcome - expected_score);
            loss += binary_cross_entropy(expected_score, outcome);
        }

        double v = 1.0 / v_sum;
        double delta = v * g_sum;
        double new_volatility = get_new_volatility(player.deviation, player.volatility, v, delta);
        player.volatility = new_volatility;
        double new_deviation = 1 / std::sqrt(1 / std::pow(player.deviation, 2) + 1 / std::pow(new_volatility, 2));
        player.deviation = 1 / std::sqrt(1 / (new_deviation * new_deviation + 1 / v));
        player.rating += new_deviation * new_deviation * g_sum;
    }

    return loss / (2 * player_ids.size());

}

double Glicko2::update_ratings(const std::string& games_file, int month, int num_threads, bool output_ratings) {

    // 1. Get player games
    std::map<int, std::vector<std::pair<int, double>>> player_games = get_player_games(games_file);

    // 2. Collect all player IDs and ensure deviations are updated
    std::vector<int> player_ids = get_player_ids(player_games);

    // 3. Update ratings and deviations to Glicko2 scale
    update_player_ratings_and_deviations(player_ids, month);

    // 4. Update ratings
    return update_player_ratings(player_games, player_ids, month, num_threads);
    
}
