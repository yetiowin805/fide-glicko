use std::fs::File;
use std::io;
use std::path::Path;

fn g(rd: f64) -> f64 {
    1.0 / (1.0 + 3.0 * rd.powi(2) / std::f64::consts::PI.powi(2)).sqrt()
}

fn expected_score(r_i: f64, rd_i: f64, r_j: f64, rd_j: f64) -> f64 {
    let combined_rd = (rd_i.powi(2) + rd_j.powi(2)).sqrt();
    let g_combined = g(combined_rd);
    let delta_r = (r_i - r_j) / 400.0;
    
    1.0 / (1.0 + 10f64.powf(-g_combined * delta_r))
}

fn binary_cross_entropy_loss(r_i: f64, rd_i: f64, r_j: f64, rd_j: f64, outcome: f64) -> f64 {
    // Ensure the outcome is either 0.0 (loss), 1.0 (win), or 0.5 (draw)
    assert!(outcome == 0.0 || outcome == 1.0 || outcome == 0.5, "Outcome must be 0.0, 1.0, or 0.5");

    let p_a = expected_score(r_i, rd_i, r_j, rd_j);

    // Binary cross-entropy loss
    -outcome * p_a.ln() - (1.0 - outcome) * (1.0 - p_a).ln()
}

fn calculate_mean_binary_cross_entropy(file_path: &str) -> io::Result<f64> {
    // Open the file
    let path = Path::new(file_path);
    let file = File::open(&path)?;
    let mut rdr = csv::ReaderBuilder::new().has_headers(false).from_reader(file);

    let mut total_loss = 0.0;
    let mut count = 0;

    // Process each record in the CSV file
    for result in rdr.records() {
        let record = result?; // Unwrap the record or return the error

        if record.len() != 5 {
            eprintln!("Invalid line format: {:?}", record);
            continue;
        }

        // Parse ratings and outcome
        let r_i: f64 = record[0].parse().expect("Invalid rating for player 1");
        let rd_i: f64 = record[1].parse().expect("Invalid deviation for player 1");
        let r_j: f64 = record[2].parse().expect("Invalid rating for player 2");
        let rd_j: f64 = record[3].parse().expect("Invalid deviation for player 2");
        let outcome: f64 = record[4].parse().expect("Invalid outcome");

        // Compute binary cross-entropy loss
        let loss = binary_cross_entropy_loss(r_i, rd_i, r_j, rd_j, outcome);
        total_loss += loss;
        count += 1;
    }

    if count == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "No valid games found"));
    }

    Ok(total_loss / count as f64)
}

fn main() {
    let file_path = "games.csv"; // Replace with your file path

    match calculate_mean_binary_cross_entropy(file_path) {
        Ok(mean_loss) => println!("Mean Binary Cross-Entropy Loss: {:.6}", mean_loss),
        Err(e) => eprintln!("Error calculating mean loss: {}", e),
    }
}
