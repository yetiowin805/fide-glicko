#include <stdio.h>
#include <stdlib.h>
#define _USE_MATH_DEFINES
#include <math.h>

#define BASE_RATING 1500.0
#define BASE_RD 350.0
#define BASE_VOLATILITY 0.06
#define TAU 0.2
#define MAX_PLAYERS 500000
#define MAX_GAMES 100
#define TABLE_SIZE 1000003  // Size of the hash table
#define PHI 0x9e3779b9

typedef struct {
    int opponent_id;
    double score;
} GameResult;

typedef struct Player {
    int id;
    double rating;
    double rd;
    double volatility;
    double new_rating;  // New field
    double new_rd;      // New field
    struct Player* next; // For linked list in case of collision
    GameResult* games;   // Dynamic array for game results
    int games_count;
} Player;

Player* hash_table[TABLE_SIZE];

unsigned int hash_function(int id) {
    double val = (double)id * PHI;
    return ((unsigned int)(val * TABLE_SIZE)) % TABLE_SIZE;
}

void insert(Player player) {
    unsigned int index = hash_function(player.id);
    
    Player* new_player = (Player*)malloc(sizeof(Player));
    if (!new_player) {
        printf("Error: Memory allocation failed for player with ID %d. Exiting...\n", player.id);
        exit(1);
    }
    
    *new_player = player;
    new_player->next = NULL;
    new_player->games = NULL;
    new_player->games_count = 0;
    
    if (!hash_table[index]) {
        hash_table[index] = new_player;
    } else {
        Player* current = hash_table[index];
        while (current->next) {
            current = current->next;
        }
        current->next = new_player;
    }
}

Player* retrieve(int id) {
    unsigned int index = hash_function(id);
    Player* current = hash_table[index];
    while (current && current->id != id) {
        current = current->next;
    }
    return current;
}

void add_game(int id, GameResult game) {
    Player* player = retrieve(id);
    if (!player) {
        // Player not found, create a default player
        Player new_player;
        new_player.id = id;
        new_player.rating = 1500.0;  // Default rating value
        new_player.rd = 350.0;       // Default RD value
        new_player.volatility = 0.06;  // Default volatility value
        new_player.games_count = 0;
        new_player.games = NULL;
        insert(new_player);
        
        player = retrieve(id);  // Retrieve the newly inserted player
        if (!player) {
            // Error handling, in case the insertion failed for some reason
            printf("Error: Unable to create a default player for ID: %d.\n", id);
            return;
        }
    }

    player->games_count++;
    player->games = (GameResult*)realloc(player->games, player->games_count * sizeof(GameResult));
    player->games[player->games_count - 1] = game;
}

double f(double x, double delta, double v, double A) {
    double ex = exp(x);
    return (ex * (delta * delta - v - ex)) / (2 * pow(v + ex, 2)) - (x - A) / pow(TAU, 2);
}

double g(double RD) {
    return 1.0 / sqrt(1.0 + (3.0 * pow(RD, 2.0)) / pow(M_PI, 2.0));
}

double E(double mu, double mu_j, double RD_j) {
    return 1.0 / (1.0 + exp(-g(RD_j) * (mu - mu_j)));
}

void glicko2_update(Player* target, GameResult* games, int game_count, Player* players, int player_count) {
    double mu = (target->rating - 1500.0) / (173.7178);
    double phi = target->rd / 173.7178;

    double v_inv = 0.0;
    double delta_sum = 0.0;

    for (int i = 0; i < game_count; i++) {
        Player* opponent = retrieve(games[i].opponent_id);
        if (!opponent) continue;

        double mu_j = (opponent->rating - 1500.0) / 173.7178;
        double phi_j = opponent->rd / 173.7178;
        double score = games[i].score;

        v_inv += pow(g(phi_j), 2.0) * E(mu, mu_j, phi_j) * (1 - E(mu, mu_j, phi_j));
        delta_sum += g(phi_j) * (score - E(mu, mu_j, phi_j));
    }

    double v = 1.0 / v_inv;
    double delta = v * delta_sum;

    double a = log(pow(target->volatility, 2.0));
    double A = a;
    double B;

    if (pow(delta, 2.0) > pow(phi, 2.0) + v) {
        B = log(pow(delta, 2.0) - pow(phi, 2.0) - v);
    } else {
        int k = 1;
        while (f(a - k * TAU, delta, v, A) < 0) {
            k++;
        }
        B = a - k * TAU;
    }

    double epsilon = 0.000001;
    double fa = f(A, delta, v, A);
    double fb = f(B, delta, v, A);

    while (fabs(B - A) > epsilon) {
        double C = A + (A - B) * fa / (fb - fa);
        double fc = f(C, delta, v, A);

        if (fc * fb < 0) {
            A = B;
            fa = fb;
        } else {
            fa /= 2;
        }
        B = C;
        fb = fc;
    }

    double new_volatility = exp(A / 2.0);
    // Log extreme cases
    if (new_volatility > 1.0) {
        FILE *log_file = fopen("extreme_cases.log", "a");  // Open the file in append mode
        if (!log_file) {
            printf("Error: Unable to open extreme_cases.log for writing.\n");
            return;
        }

        fprintf(log_file, "Extreme case detected for Player FIDE ID: %d\n", target->id);
        fprintf(log_file, "Rating: %lf, RD: %lf, Volatility: %lf\n", target->rating, target->rd, target->volatility);
        fprintf(log_file, "New Rating: %lf, New RD: %lf, New Volatility: %lf\n", target->new_rating, target->new_rd, target->volatility);
        for (int i = 0; i < game_count; i++) {
            fprintf(log_file, "Game against Player ID: %d, Score: %lf\n", games[i].opponent_id, games[i].score);
        }
        fprintf(log_file, "mu: %lf, phi: %lf, v: %lf, delta: %lf, new_volatility: %lf\n", mu, phi, v, delta, new_volatility);
        fprintf(log_file, "-----------------------------------------\n");

        fclose(log_file);  // Close the file
    }
    // hem in extreme cases when they occur
    if (new_volatility > 2.8){
        new_volatility = 2.8;
    }
    double phi_star = sqrt(pow(phi, 2.0) + pow(new_volatility, 2.0));
    double new_phi = 1.0 / sqrt(1.0 / pow(phi_star, 2.0) + 1.0 / v);
    // Log extreme cases
    if (new_phi < 0.1 || new_phi > 2.8) {
        FILE *log_file = fopen("extreme_cases.log", "a");  // Open the file in append mode
        if (!log_file) {
            printf("Error: Unable to open extreme_cases.log for writing.\n");
            return;
        }

        fprintf(log_file, "Extreme case detected for Player FIDE ID: %d\n", target->id);
        fprintf(log_file, "Rating: %lf, RD: %lf, Volatility: %lf\n", target->rating, target->rd, target->volatility);
        fprintf(log_file, "New Rating: %lf, New RD: %lf, New Volatility: %lf\n", target->new_rating, target->new_rd, target->volatility);
        for (int i = 0; i < game_count; i++) {
            fprintf(log_file, "Game against Player ID: %d, Score: %lf\n", games[i].opponent_id, games[i].score);
        }
        fprintf(log_file, "mu: %lf, phi: %lf, v: %lf, delta: %lf, new_phi: %lf, new_volatility: %lf\n", mu, phi, v, delta, new_phi, new_volatility);
        fprintf(log_file, "-----------------------------------------\n");

        fclose(log_file);  // Close the file
    }
    // hem in extreme cases when they occur
    if (new_phi > 2.9){
        new_phi = 2.9;
    }
    double new_mu = mu + pow(new_phi, 2.0) * delta_sum;

    target->new_rating = new_mu * 173.7178 + 1500.0;
    target->new_rd = new_phi * 173.7178;
    target->volatility = new_volatility;
}

void apply_new_ratings() {
    for (int i = 0; i < TABLE_SIZE; i++) {
        Player* current = hash_table[i];
        while (current) {
            current->rating = current->new_rating;
            current->rd = current->new_rd;
            current = current->next;
        }
    }
}

void print_progress_bar(int current, int total) {
    int bar_length = 50;
    int position = bar_length * current / total;
    printf("[");
    for (int i = 0; i < bar_length; i++) {
        if (i < position) printf("=");
        else if (i == position) printf(">");
        else printf(" ");
    }
    printf("] %d%%\r", (int)(100 * current / total));
    fflush(stdout);
}

void write_to_file(const char* filename) {
    FILE* out_file = fopen(filename, "w");
    if (!out_file) {
        printf("Error: Unable to open %s for writing.\n", filename);
        return;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        Player* current = hash_table[i];
        while (current) {
            fprintf(out_file, "%d %lf %lf %lf\n", current->id, current->rating, current->rd, current->volatility);
            current = current->next;
        }
    }
    fclose(out_file);
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s <ratings_file> <games_file> <output_file>\n", argv[0]);
        return 1;
    }

    const char* ratings_filename = argv[1];
    const char* games_filename = argv[2];
    const char* output_filename = argv[3];

    printf("Opening %s...\n", ratings_filename);
    FILE* rating_file = fopen(ratings_filename, "r");
    if (!rating_file) {
        printf("Error: Unable to open %s.\n", ratings_filename);
        return 1;
    }

    printf("Reading player ratings...\n");
    int players_count = 0;
    Player player;

    // Count the number of lines in the file for the progress bar
    int total_players = 0;
    char ch;
    while (!feof(rating_file)) {
        ch = fgetc(rating_file);
        if (ch == '\n') {
            total_players++;
        }
    }
    rewind(rating_file);  // Reset the file pointer to the beginning

    while (fscanf(rating_file, "%d %lf %lf %lf", &player.id, &player.rating, &player.rd, &player.volatility) == 4) {
        insert(player);
        players_count++;
        print_progress_bar(players_count, total_players);
    }


    FILE* game_file = fopen(games_filename, "r");
    if (!game_file) {
        printf("Error: Unable to open %s.\n", games_filename);
        return 1;
    }

    printf("\nReading games...\n");

    // Count the number of lines in the game file for the progress bar
    int total_games = 0;
    while (!feof(game_file)) {
        ch = fgetc(game_file);
        if (ch == '\n') {
            total_games++;
        }
    }
    rewind(game_file);  // Reset the file pointer to the beginning

    int games_processed = 0;
    int id, count;
    while (fscanf(game_file, "%d %d", &id, &count) == 2) {
        for (int j = 0; j < count; j++) {
            GameResult game;
            if (fscanf(game_file, "%d %lf", &game.opponent_id, &game.score) != 2) {
                printf("Error: Expected game data for player %d. Exiting...\n", id);
                fclose(game_file);
                return 1;
            }
            add_game(id, game);
            games_processed++;
            print_progress_bar(games_processed, total_games);
        }
        games_processed++;
    }
    fclose(game_file);

    printf("\nUpdating player ratings...\n");
    int updated_players = 0;
    for (int i = 0; i < TABLE_SIZE; i++) {
        Player* current = hash_table[i];
        while (current) {
            glicko2_update(current, current->games, current->games_count, NULL, 0);
            current = current->next;
            updated_players++;
            print_progress_bar(updated_players, players_count);
        }
    }
    printf("\n");

    apply_new_ratings();

    // Write updated ratings to the output file
    write_to_file(output_filename);
    printf("Results written to %s\n", output_filename);
    return 0;
}