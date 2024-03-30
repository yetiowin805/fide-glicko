#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEFAULT_RATING 1500
#define DEFAULT_RD 350
#define DEFAULT_VOLATILITY 0.09

typedef struct {
    int id;
    char name[100];
    int rating;
    double rd;
    double volatility;
} Player;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s <filename>\n", argv[0]);
        return 1;
    }

    FILE* file = fopen(argv[1], "r");
    if (!file) {
        printf("Error: Unable to open %s.\n", argv[1]);
        return 1;
    }

    FILE* out_file = fopen("output2.txt", "w");
    if (!out_file) {
        printf("Error: Unable to open output2.txt for writing.\n");
        fclose(file);
        return 1;
    }

    char line[500];
    fgets(line, sizeof(line), file);  // Read and discard the header line

    Player player;
    while (fgets(line, sizeof(line), file)) {
        memset(&player, 0, sizeof(player));

        sscanf(line, "%d", &player.id);

        // Extract player name (though we won't be using it in the output)
        strncpy(player.name, line + 12, 33);
        char *end = player.name + strlen(player.name) - 1;
        while(end > player.name && (*end == ' ' || *end == '\t' || *end == '\n')) {
            *end = '\0';
            end--;
        }

        // Extract rating
        char temp[10];  // Assuming a maximum of 9 characters for the rating
        strncpy(temp, line + 59, 8);
        temp[9] = '\0';  // Null-terminate the string
        if (sscanf(temp, "%d", &player.rating) != 1 || player.rating < 1000 || player.rating > 3000) {
            fprintf(stderr, "Error: Invalid rating encountered: '%s'\n", temp);
            exit(EXIT_FAILURE);  // Exit the program with an error code
        }
        strncpy(temp, line+82, 4);
        temp[4] = '\0';
        if (strchr(temp, 'i') == NULL) {
            if (player.rating > 2500) {
                player.rd = 50;
            } else {
                player.rd = DEFAULT_RD;
            }
        } else {
            if (player.rating > 2500) {
                player.rd = 90;
            } else {
                player.rd = DEFAULT_RD;
            }
        }
        player.volatility = DEFAULT_VOLATILITY;

        // Write the player's data to the output file in the specified format
        fprintf(out_file, "%d %.3f %.3f %.3f\n", player.id, (float)player.rating, player.rd, player.volatility);
    }

    fclose(file);
    fclose(out_file);
    printf("Results written to output2.txt\n");
    return 0;
}
