#include <iostream>
#include <map>
#include <vector>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <omp.h>

using namespace std;

struct Player {
    int num_opponents;
    vector<int> opponents_fide;
    vector<double> results;
};

int total_files = 0;
int processed_files = 0;

void merge_players(int fide_id1, int fide_id2, map<int, Player>& local_players_map) {
    Player& player1 = local_players_map[fide_id1];
    Player& player2 = local_players_map[fide_id2];

    player1.opponents_fide.insert(player1.opponents_fide.end(), 
                                  player2.opponents_fide.begin(), 
                                  player2.opponents_fide.end());

    player1.results.insert(player1.results.end(), 
                           player2.results.begin(), 
                           player2.results.end());
}

void display_progress_bar() {
    const int bar_width = 50;
    double progress = (double)processed_files / total_files;
    printf("\r[");
    int pos = bar_width * progress;
    for (int i = 0; i < bar_width; ++i) {
        if (i < pos) printf("=");
        else if (i == pos) printf(">");
        else printf(" ");
    }
    printf("] %d%%", (int)(progress * 100));
    fflush(stdout);
}

void count_files_in_directory(const char *dir_path) {
    struct dirent *entry;
    DIR *dp = opendir(dir_path);
    if (dp == NULL) {
        perror("Failed to open directory");
        return;
    }

    while ((entry = readdir(dp))) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {  // ignore '.' and '..'
            char new_path[1024];
            snprintf(new_path, sizeof(new_path), "%s/%s", dir_path, entry->d_name);

            struct stat path_stat;
            stat(new_path, &path_stat);

            if (S_ISREG(path_stat.st_mode)) {  // if it's a file
                total_files++;
            } else if (S_ISDIR(path_stat.st_mode)) {  // if it's a directory
                count_files_in_directory(new_path);
            }
        }
    }

    closedir(dp);
}

void process_file(const char *path, map<int, Player>& local_players_map) {
    FILE *file = fopen(path, "r");
    printf("Processing %s\n", path);
    if (!file) {
        printf("Failed to open the file %s.\n", path);
        return;
    }

    int fide_id, n;
    double result;

    while (fscanf(file, "%d %d", &fide_id, &n) != EOF) {
        Player &player = local_players_map[fide_id];

        for (int i = 0; i < n; ++i) {
            int opponent_fide;
            fscanf(file, "%d %lf", &opponent_fide, &result);
            player.opponents_fide.push_back(opponent_fide);
            player.results.push_back(result);
        }
    }

    fclose(file);

    file = fopen(path, "w");
    if (!file) {
        printf("Failed to open the file %s for writing.\n", path);
        return;
    }

    for (auto &entry : local_players_map) {
        int fide_id = entry.first;
        Player &player = entry.second;
        fprintf(file, "%d %zu\n", fide_id, player.opponents_fide.size());
        for (size_t j = 0; j < player.opponents_fide.size(); ++j) {
            fprintf(file, "%d %.1lf\n", player.opponents_fide[j], player.results[j]);
        }
    }

    fclose(file);

    processed_files++;
    display_progress_bar();
}

void process_directory(const char *dir_path) {
    struct dirent *entry;
    DIR *dp = opendir(dir_path);
    printf("Processing %s\n", dir_path);
    if (dp == NULL) {
        perror("Failed to open directory");
        return;
    }

    vector<string> paths;

    while ((entry = readdir(dp))) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            char new_path[1024];
            snprintf(new_path, sizeof(new_path), "%s/%s", dir_path, entry->d_name);
            paths.push_back(new_path);
        }
    }
    closedir(dp);

    #pragma omp parallel for
    for (int i = 0; i < paths.size(); i++) {
        struct stat path_stat;
        stat(paths[i].c_str(), &path_stat);

        if (S_ISREG(path_stat.st_mode)) {
            map<int, Player> local_players_map;
            process_file(paths[i].c_str(), local_players_map);
        } else if (S_ISDIR(path_stat.st_mode)) {
            process_directory(paths[i].c_str());
        }

        #pragma omp atomic
        processed_files++;
        display_progress_bar();
    }
}


int main() {
    const char *root_dir = "./clean_numerical";
    count_files_in_directory(root_dir);
    process_directory(root_dir);
    cout << "\nAll files processed and updated!" << endl;
    return 0;
}