#ifndef GLICKO2_H
#define GLICKO2_H

#include <map>
#include <string>

class Glicko2 {
public:

    struct Player {
        double rating;
        double deviation;
        double volatility;
        int last_month_played;
    };

    Glicko2() = default;
    Glicko2(double starting_deviation, double starting_volatility, double tau, double epsilon)
        : starting_deviation(starting_deviation), starting_volatility(starting_volatility), tau(tau), epsilon(epsilon) {}
    ~Glicko2() = default;

    // Parquet file containing the player ratings, deviation, volatility, and last month played
    void set_player_ratings(const std::string& ratings_file);
    // Given a parquet file of games, update the ratings of the players
    // Returns the binary cross-entropy loss of the rating predictions
    // If output_ratings is true, also outputs a file containing the new player ratings
    double update_ratings(const std::string& games_file, int month, int num_threads = 1, bool output_ratings = false);

    // For testing purposes only
    const std::map<int, Player>& get_players() const { return players; }

private:

    double starting_deviation{350.0};
    double starting_volatility{0.06};
    double tau{0.6};
    double epsilon{1e-6};
    std::map<int, Player> players;

    // Helper functions
    std::map<int, std::vector<std::pair<int, double>>>
    get_player_games(const std::string &games_file);
    std::vector<int> get_player_ids(
        const std::map<int, std::vector<std::pair<int, double>>> &player_games);
    void update_player_ratings_and_deviations(const std::vector<int> &player_ids, int month);
    double update_player_ratings(
        const std::map<int, std::vector<std::pair<int, double>>> &player_games,
        const std::vector<int> &player_ids, int month, int num_threads);
    double f(double x, double a, double deviation, double v, double delta) const;
    double get_new_volatility(double deviation, double volatility, double v, double delta) const;
    const double DEFAULT_RATING = 1500.0;


    
};

#endif // GLICKO_H
