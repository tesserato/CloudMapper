use clap::Parser;
use rayon::prelude::*;

use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::Instant;

// --- Configuration Constants (Base Filenames) ---
const TREE_OUTPUT_FILE_NAME: &str = "files.txt";
const DUPLICATES_OUTPUT_FILE_NAME: &str = "duplicates.txt";
const SIZE_OUTPUT_FILE_NAME: &str = "size_used.txt";
const ENABLE_DUPLICATES_REPORT: bool = true;
// --- End Configuration Constants ---

// --- Command Line Argument Definition ---
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the rclone executable (optional)
    #[arg(long, short = 'r', env = "RCLONE_EXECUTABLE")]
    rclone_path: Option<String>,

    /// Path to the rclone config file (optional)
    #[arg(long, short = 'c', env = "RCLONE_CONFIG")]
    rclone_config: Option<String>,

    /// Path to the directory for output reports
    #[arg(long, short = 'o', env = "RCLONE_ANALYZER_OUTPUT", default_value = "./cloud")]
    output_path: String,
    /// Enable duplicate file detection report
    #[arg(long, short = 'd', default_value_t = true)] // Or false if default off
    duplicates: bool,
}

// Helper to run a command
fn run_command(executable: &str, args: &[&str]) -> Result<Output, io::Error> {
    let args_display = args
        .iter()
        .map(|&a| {
            if a.contains(' ') {
                format!("\"{}\"", a)
            } else {
                a.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    println!("> Running: {} {}", executable, args_display);
    let output = Command::new(executable)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    match output {
        Ok(ref out) => {
            if !out.status.success() {
                eprintln!("  Command failed with status: {}", out.status);
                let stderr_str = String::from_utf8_lossy(&out.stderr);
                if !stderr_str.is_empty() {
                    eprintln!("  stderr:\n---\n{}\n---", stderr_str.trim());
                }
            }
        }
        Err(ref e) => {
            eprintln!("  Failed to execute command '{}': {}", executable, e);
            if e.kind() == ErrorKind::NotFound {
                eprintln!(
                    "  Hint: Make sure '{}' is installed or the path is correct.",
                    executable
                );
            }
        }
    }
    output
}

// Define a type alias for the result of processing a single remote
type RemoteProcessingResult = Result<(String, Vec<cloudmapper::RawFile>), (String, String)>;
//                             Ok(remote_name, files)      Err(remote_name, error_message)

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Parse Command Line Arguments ---
    let args = Args::parse();

    // --- Determine rclone executable path ---
    // Use provided path, or default to "rclone"
    let rclone_executable = args.rclone_path.as_deref().unwrap_or("rclone");
    println!("Using rclone executable: {}", rclone_executable);
    if let Some(conf) = &args.rclone_config {
        println!("Using rclone config: {}", conf);
    }
    println!("Output directory: {}", args.output_path);

    println!("Starting rclone data processing...");
    let overall_start_time = Instant::now();

    // --- Create Output Directory ---
    // Ensure the output directory exists, create it if not.
    fs::create_dir_all(&args.output_path)?;
    println!("Output directory '{}' ensured.", &args.output_path);

    // --- 1. Initialize Files container from lib ---
    let mut files_collection = cloudmapper::Files::new();

    // --- 2. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");

    // Build common args (potentially including --config)
    let mut common_rclone_args: Vec<String> = Vec::new();
    if let Some(config_path) = &args.rclone_config {
        common_rclone_args.push("--config".to_string()); // Push String
        common_rclone_args.push(config_path.clone()); // Push String
    }

    let mut list_remotes_args_for_cmd: Vec<&str> = common_rclone_args
        .iter()
        .map(|s| s.as_ref()) // Use as_ref()
        .collect();
    list_remotes_args_for_cmd.push("listremotes"); // Push &str

    let remote_names: Vec<String> = match run_command(rclone_executable, &list_remotes_args_for_cmd)
    {
        Ok(output) => {
            if !output.status.success() {
                return Err(Box::new(io::Error::new(
                    ErrorKind::Other,
                    format!("'{} listremotes' failed.", rclone_executable),
                )));
            }
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .map(String::from)
                .collect()
        }
        Err(e) => {
            return Err(Box::new(e));
        }
    };

    if remote_names.is_empty() {
        println!("No rclone remotes found or 'rclone listremotes' failed. Exiting.");
        return Ok(());
    }

    println!("Found {} remotes: {:?}", remote_names.len(), remote_names);
    println!("==========================================");

    // --- 3. Process Remotes in Parallel ---
    println!("Processing remotes in parallel...");
    let loop_start_time = Instant::now();

    // Use rayon's par_iter to map over remote names in parallel.
    // Collect results into a Vec.
    let results: Vec<RemoteProcessingResult> = remote_names
        .par_iter() // Create parallel iterator
        .map(|remote_name| {
            // This closure runs potentially in parallel for each remote_name
            println!("  Starting processing for: {}", remote_name);
            let start = Instant::now();

            // Clone common args (which are Vec<String>) for this thread/task
            let mut lsjson_args_owned: Vec<String> = common_rclone_args.clone();

            // Extend the Vec<String> with more Strings
            lsjson_args_owned.extend(vec![
                "lsjson".to_string(),
                "-R".to_string(),
                "--hash".to_string(),
                "--fast-list".to_string(),
                remote_name.clone(), // remote_name is &String, clone gives String
            ]);

            // Convert Vec<String> to Vec<&str> *just* for the run_command call
            let lsjson_args_for_cmd: Vec<&str> = lsjson_args_owned
                .iter()
                .map(|s| s.as_ref()) // Use as_ref()
                .collect();

            // Execute the command
            match run_command(rclone_executable, &lsjson_args_for_cmd) {
                // Pass Vec<&str>
                Ok(output) => {
                    // ... (rest of the match arm remains the same)
                    if output.status.success() {
                        let json_string = String::from_utf8_lossy(&output.stdout);
                        // Parse JSON
                        match cloudmapper::parse_rclone_lsjson(&json_string) {
                            Ok(raw_files) => {
                                let duration = start.elapsed();
                                println!(
                                    "  Finished processing {} ({} items) in {:.2}s",
                                    remote_name,
                                    raw_files.len(),
                                    duration.as_secs_f32()
                                );
                                // Success: Return remote name and parsed files
                                Ok((remote_name.clone(), raw_files))
                            }
                            Err(e) => {
                                // JSON parsing error
                                let err_msg = format!(
                                    "Error parsing JSON for remote '{}': {}",
                                    remote_name, e
                                );
                                eprintln!("  {}", err_msg);
                                // Failure: Return remote name and error message
                                Err((remote_name.clone(), err_msg))
                            }
                        }
                    } else {
                        // rclone command failed
                        let err_msg = format!(
                            "rclone command failed for remote '{}' with status {}",
                            remote_name, output.status
                        );
                        // stderr already printed by run_command
                        eprintln!("  {}", err_msg);
                        // Failure: Return remote name and error message
                        Err((remote_name.clone(), err_msg))
                    }
                }
                Err(e) => {
                    // Failed to execute rclone command
                    let err_msg = format!(
                        "Error executing rclone command for remote '{}': {}",
                        remote_name, e
                    );
                    // stderr already printed by run_command
                    eprintln!("  {}", err_msg);
                    // Failure: Return remote name and error message
                    Err((remote_name.clone(), err_msg))
                }
            }
        })
        .collect(); // Collect results from all parallel tasks into a Vec

    let loop_duration = loop_start_time.elapsed();
    println!(
        "Finished parallel processing phase in {:.2}s.",
        loop_duration.as_secs_f32()
    );
    println!("==========================================");
    println!("Aggregating results...");

    // --- 3b. Aggregate Results Sequentially ---
    // Iterate through the collected results sequentially to safely update shared state
    let mut process_errors: u32 = 0;
    let mut remotes_processed_successfully: u32 = 0;
    for result in results {
        match result {
            Ok((remote_name, raw_files)) => {
                // Add parsed data using lib function
                files_collection.add_remote_files(&remote_name, raw_files);
                remotes_processed_successfully += 1;
                // Success message already printed in the parallel task
            }
            Err((_remote_name, _error_message)) => {
                // Error message already printed in the parallel task
                process_errors += 1;
            }
        }
    }

    // --- 4. Generate Reports ( uses aggregated data) ---
    if files_collection.files.is_empty() {
        if process_errors > 0 {
            println!(
                "No file data was collected due to processing errors. Skipping report generation."
            );
        } else if remotes_processed_successfully == 0 && !remote_names.is_empty() {
            // This case might mean listremotes worked but all lsjson calls failed before parsing
            println!("No data successfully processed from any remote. Skipping report generation.");
        } else {
            println!("No files found across any successfully processed remotes. Skipping report generation.");
        }
    } else {
        println!("Generating final reports...");
        let report_start_time = Instant::now();

        // Construct full output paths using the provided directory
        let output_dir = PathBuf::from(&args.output_path);
        let tree_output_path = output_dir.join(TREE_OUTPUT_FILE_NAME);
        let duplicates_output_path = output_dir.join(DUPLICATES_OUTPUT_FILE_NAME);
        let size_output_path = output_dir.join(SIZE_OUTPUT_FILE_NAME);

        match cloudmapper::generate_reports(
            &mut files_collection,
            args.duplicates,
            tree_output_path.to_str().unwrap_or_default(), // Convert PathBuf to &str
            duplicates_output_path.to_str().unwrap_or_default(),
            size_output_path.to_str().unwrap_or_default(),
        ) {
            Ok(_) => {
                println!(
                    "Reports generated successfully in {:.2}s.",
                    report_start_time.elapsed().as_secs_f32()
                );
                println!("  Tree report: {}", tree_output_path.display());
                println!("  Size report: {}", size_output_path.display());
                if ENABLE_DUPLICATES_REPORT {
                    println!("  Duplicates report: {}", duplicates_output_path.display());
                }
            }
            Err(e) => {
                eprintln!("Error generating final reports: {}", e);
                process_errors += 1; // Count report generation failure as an error
            }
        }
    }

    // --- 5. Print Summary ---
    // (Remains the same)
    let total_duration = overall_start_time.elapsed();
    println!("==========================================");
    println!("Processing Summary:");
    println!("  Total time: {:.2}s", total_duration.as_secs_f32());
    println!("  Remotes found: {}", remote_names.len());
    println!(
        "  Remotes processed successfully: {}",
        remotes_processed_successfully
    );
    println!("  Processing errors encountered: {}", process_errors);
    println!("==========================================");

    if process_errors > 0 {
        eprintln!("Completed with errors.");
        return Err(Box::new(io::Error::new(
            ErrorKind::Other,
            "Processing completed with errors",
        )));
    }

    println!("Processing completed successfully.");
    Ok(())
}
