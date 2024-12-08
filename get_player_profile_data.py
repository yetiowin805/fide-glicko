def extract_player_info(input_filename, output_filename):
    with open(input_filename, "r") as f:
        lines = f.readlines()

    # Skip the header line
    lines = lines[1:]

    extracted_data = []

    for line in lines:
        # Extract relevant fields
        fide_id = line[:9].strip()
        name = line[9:64].strip()
        federation = line[76:79].strip()
        sex = line[80:81].strip()
        b_day = line[-11:-7].strip()

        # Append extracted data to the result list
        extracted_data.append((fide_id, name, federation, sex, b_day))


# Test with the provided table file
input_filename = "players_list_foa.txt"
output_filename = "output.txt"
extract_player_info(input_filename, output_filename)
