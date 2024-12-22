use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use futures_util::StreamExt; // For chunk-by-chunk reading
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use serde::{Deserialize, Serialize};
use simplelog::{ConfigBuilder, TermLogger, TerminalMode, ColorChoice, LevelFilter};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Write; // Added Write trait
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

/// Structure representing a game record.
#[derive(Debug, Deserialize, Serialize)]
struct GameRecord {
    player1: String,
    player2: String,
    outcome: f32,
    month: String,
    time_control: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    init_logging();

    // Create a Tokio runtime
    let rt = Runtime::new()?;
    rt.block_on(async_main())?;

    Ok(())
}

/// Main async workflow
async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    // Configuration
    let bucket = "sqlite-chess-data";      // S3 bucket name
    let prefix = "game-data/";             // S3 prefix for game data
    let aws_region = "us-east-2";          // AWS region

    // Initialize AWS S3 client
    let region_provider = RegionProviderChain::default_provider().or_else(aws_region);
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    // List all Parquet files under the prefix
    info!("Listing all Parquet files in s3://{}/{}", bucket, prefix);
    let parquet_keys = list_parquet_files(&client, bucket, prefix).await?;

    if parquet_keys.is_empty() {
        info!("No Parquet files found under s3://{}/{}", bucket, prefix);
        return Ok(());
    }

    info!("Found {} Parquet files to process.", parquet_keys.len());

    // Initialize statistics
    let stats = Arc::new(Mutex::new(Statistics::new()));

    // Process each Parquet file
    for key in parquet_keys {
        info!("Processing file: {}", key);
        match download_and_process_parquet(&client, bucket, &key, &stats).await {
            Ok(_) => info!("Successfully processed: {}", key),
            Err(e) => error!("Failed to process {}: {}", key, e),
        }
    }

    // Print out the aggregated statistics
    print_statistics(&stats.lock().unwrap());

    Ok(())
}

/// Lists all Parquet files under a given S3 prefix.
async fn list_parquet_files(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut continuation_token = None;
    let mut keys = Vec::new();

    loop {
        let resp = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .set_continuation_token(continuation_token.clone())
            .send()
            .await?;

        if let Some(contents) = resp.contents {
            for obj in contents {
                if let Some(key) = obj.key {
                    if key.ends_with(".parquet") || key.ends_with(".parquet.zst") {
                        keys.push(key);
                    }
                }
            }
        }

        if resp.is_truncated {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(keys)
}

/// Downloads a Parquet file from S3 and processes it to update statistics.
async fn download_and_process_parquet(client: &Client, bucket: &str, key: &str, stats: &Arc<Mutex<Statistics>>) -> Result<(), Box<dyn std::error::Error>> {
    // Download the file to a temporary location
    let temp_dir = env::temp_dir();
    let file_name = Path::new(key).file_name().unwrap().to_str().unwrap();
    let local_path = temp_dir.join(file_name);

    // Initiate GET request
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            error!("S3 get_object request failed for {}: {}", key, e);
            e
        })?;

    // Retrieve content length if available
    let content_length = resp.content_length() as u64; // Ensure u64 type
    info!("Downloading {} ({} bytes)", key, content_length);

    // Set up a progress bar
    let pb = if content_length > 0 {
        ProgressBar::new(content_length)
    } else {
        ProgressBar::new_spinner()
    };
    let style = if content_length > 0 {
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-")
    } else {
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {bytes}")
            .unwrap()
    };
    pb.set_style(style);

    // Stream the data and write to the local file
    let mut file = File::create(&local_path)?;
    let mut stream = resp.body;
    let mut downloaded = 0;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                file.write_all(&bytes)?;
                downloaded += bytes.len() as u64;
                pb.set_position(downloaded);
            }
            Err(e) => {
                error!("Error reading chunk from S3 stream for {}: {}", key, e);
                return Err(Box::new(e));
            }
        }
    }

    pb.finish_and_clear();
    info!("Download complete for {}", key);

    // Process the Parquet file
    match process_parquet_file(&local_path, stats) {
        Ok(_) => info!("Processed Parquet file: {}", key),
        Err(e) => error!("Error processing Parquet file {}: {}", key, e),
    }

    // Optionally, remove the local file after processing to save disk space
    // std::fs::remove_file(&local_path)?;

    Ok(())
}

/// Processes a Parquet file and updates the statistics.
fn process_parquet_file(file_path: &Path, stats: &Arc<Mutex<Statistics>>) -> Result<(), Box<dyn std::error::Error>> {
    // Open the Parquet file
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    // Map column names to indices
    let mut column_map = HashMap::new();
    for i in 0..schema.num_columns() {
        let column = schema.column(i);
        column_map.insert(column.name().to_string(), i);
    }

    // Ensure required columns are present
    for &col in &["player1", "player2", "outcome", "month", "time_control"] {
        if !column_map.contains_key(col) {
            return Err(format!("Missing required column: {}", col).into());
        }
    }

    let mut iter = reader.get_row_iter(None)?;

    // Iterate over rows and update statistics
    while let Some(record) = iter.next() {
        // Access columns by index
        let player1 = record.get_string(*column_map.get("player1").unwrap()).map_or("", |v| v.as_str()).to_string();
        let player2 = record.get_string(*column_map.get("player2").unwrap()).map_or("", |v| v.as_str()).to_string();
        let outcome = record.get_float(*column_map.get("outcome").unwrap()).map_or(0.0, |v| v);
        let month = record.get_string(*column_map.get("month").unwrap()).map_or("", |v| v.as_str()).to_string();
        let time_control = record.get_string(*column_map.get("time_control").unwrap()).map_or("", |v| v.as_str()).to_string();

        let game = GameRecord {
            player1,
            player2,
            outcome,
            month,
            time_control,
        };

        let mut stats = stats.lock().unwrap();
        stats.total_rows += 1;
        *stats.rows_per_month.entry(game.month.clone()).or_insert(0) += 1;
        *stats.rows_per_time_control.entry(game.time_control.clone()).or_insert(0) += 1;
    }

    Ok(())
}

/// Structure to hold aggregated statistics.
struct Statistics {
    total_rows: usize,
    rows_per_month: HashMap<String, usize>,
    rows_per_time_control: HashMap<String, usize>,
}

impl Statistics {
    fn new() -> Self {
        Statistics {
            total_rows: 0,
            rows_per_month: HashMap::new(),
            rows_per_time_control: HashMap::new(),
        }
    }
}

/// Prints the aggregated statistics.
fn print_statistics(stats: &Statistics) {
    println!("===== Aggregated Statistics =====");
    println!("Total number of rows: {}", stats.total_rows);

    println!("\nNumber of rows per month:");
    for (month, count) in &stats.rows_per_month {
        println!("  {}: {}", month, count);
    }

    println!("\nNumber of rows per time control:");
    for (time_control, count) in &stats.rows_per_time_control {
        println!("  {}: {}", time_control, count);
    }
    println!("==================================");
}

/// Initializes a logger that prints timestamps and log levels to the terminal.
fn init_logging() {
    let mut config_builder = ConfigBuilder::new();

    // Attempt to set the time offset to local time
    if let Err(e) = config_builder.set_time_offset_to_local() {
        eprintln!("Failed to set time offset for logging: {:?}", e);
        // Continue with default time offset
    }

    // Build the logging configuration
    let config = config_builder.build();

    // Initialize the terminal logger
    if TermLogger::init(LevelFilter::Info, config, TerminalMode::Mixed, ColorChoice::Auto).is_err() {
        eprintln!("Logger initialization failed. Logging will be minimal.");
    }
}
