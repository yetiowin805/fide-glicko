#ifndef GLICKO_H
#define GLICKO_H

#include <cmath>
#include <map>
#include <string>
#include <vector>

class Glicko {
public:
  struct Player {
    double rating;
    double deviation;
    int last_month_played;
  };

  Glicko();
  Glicko(double starting_deviation, double c)
      : starting_deviation(starting_deviation), c(c) {}
  ~Glicko() = default;

  // Parquet file containing the player ratings, deviation, and last month
  // played
  void set_player_ratings(const std::string &ratings_file);
  // Given a parquet file of games, update the ratings of the players
  // Returns the binary cross-entropy loss of the rating predictions
  // If output_ratings is true, also outputs a file containing the new player
  // ratings
  double update_ratings(const std::string &games_file, int month,
                        int num_threads = 1, bool output_ratings = false);

  // For testing purposes only
  const std::map<int, Player> &get_players() const { return players; }

private:
  double starting_deviation;
  double c;
  std::map<int, Player> players;

  // Helper functions
  std::map<int, std::vector<std::pair<int, double>>>
  get_player_games(const std::string &games_file);
  std::vector<int> get_player_ids(
      const std::map<int, std::vector<std::pair<int, double>>> &player_games);
  void update_player_deviations(const std::vector<int> &player_ids, int month);
  double update_player_ratings(
      const std::map<int, std::vector<std::pair<int, double>>> &player_games,
      const std::vector<int> &player_ids, int month, int num_threads);

  const double DEFAULT_RATING = 1500.0;
};

#endif // GLICKO_H
