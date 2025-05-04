// src/main.rs
use clap::{Parser, ValueEnum}; // Import ValueEnum
use rayon::prelude::*;

use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::Instant;

// Use the enum from lib.rs
use cloudmapper::OutputDivisionMode;

// --- Configuration Constants (Base Filenames) ---
// Keep these as base names, generate_reports will join with output_dir
const TREE_OUTPUT_FILE_NAME: &str = "files.txt"; // Used only in Single mode and as content filename in Folder mode
const DUPLICATES_OUTPUT_FILE_NAME: &str = "duplicates.txt";
const SIZE_OUTPUT_FILE_NAME: &str = "size_used.txt";
const ABOUT_OUTPUT_FILE_NAME: &str = "about.txt";

const REMOTE_ICON: &str = "☁️";
const FOLDER_ICON: &str = "📁";
const FILE_ICON: &str = "📄";
const SIZE_ICON: &str = "💽";
const DATE_ICON: &str = "📆";

// --- Enum for Output Division Mode (used by Clap) ---
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "kebab-case")] // Use kebab-case for CLI args (e.g., --output-division single)
enum CliOutputDivisionMode {
    /// Output all remotes into a single file (files.txt)
    Single,
    /// Output each remote's file list into its own file (<remote_name>.txt)
    Remote,
    /// Output files by creating a directory structure mirroring the remote
    Folder,
}

// Map CLI enum to internal lib enum
impl From<CliOutputDivisionMode> for OutputDivisionMode {
    fn from(cli_mode: CliOutputDivisionMode) -> Self {
        match cli_mode {
            CliOutputDivisionMode::Single => OutputDivisionMode::Single,
            CliOutputDivisionMode::Remote => OutputDivisionMode::Remote,
            CliOutputDivisionMode::Folder => OutputDivisionMode::Folder,
        }
    }
}

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
    #[arg(
        long,
        short = 'o',
        env = "RCLONE_ANALYZER_OUTPUT",
        default_value = "./cloud"
    )]
    output_path: String,

    /// How to divide the file listing output
    #[arg(
        long,
        value_enum, // Use clap's value_enum feature
        default_value_t = CliOutputDivisionMode::Folder, // Default to folder structure
        env = "RCLONE_ANALYZER_OUTPUT_DIVISION"
    )]
    output_division: CliOutputDivisionMode,

    /// Enable duplicate file detection report
    #[arg(
        long,
        short = 'd',
        default_value_t = true,
        env = "RCLONE_ANALYZER_DUPLICATES"
    )]
    duplicates: bool,

    /// Enable the 'rclone about' report for remote sizes
    #[arg(
        long,
        short = 'a',
        default_value_t = true,
        env = "RCLONE_ANALYZER_ABOUT"
    )]
    about_report: bool,

    /// Clean the output directory before generating reports
    #[arg(
        long,
        short = 'C', // Capital C for Clean
        default_value_t = true,
        env = "RCLONE_ANALYZER_CLEAN_OUTPUT"
    )]
    clean_output: bool,
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
    println!("Output division mode: {:?}", args.output_division); // Print new flag
    println!("Clean output directory: {}", args.clean_output);
    println!("Duplicates report enabled: {}", args.duplicates);
    println!("About report enabled: {}", args.about_report);

    println!("Starting rclone data processing...");
    let overall_start_time = Instant::now();

    // --- Create/Clean Output Directory ---
    let output_dir = PathBuf::from(&args.output_path); // Store as PathBuf
    if args.clean_output && output_dir.exists() {
        println!("Cleaning output directory '{}'...", output_dir.display());
        fs::remove_dir_all(&output_dir)?;
        println!("Output directory cleaned.");
    }
    fs::create_dir_all(&output_dir)?;
    println!("Output directory '{}' ensured.", output_dir.display());

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
        .map(|s| s.as_str()) // Convert String to &str
        .collect();
    list_remotes_args_for_cmd.push("listremotes"); // Push &str

    // Use run_command from lib.rs
    let remote_names: Vec<String> =
        match cloudmapper::run_command(rclone_executable, &list_remotes_args_for_cmd) {
            Ok(output) => {
                if !output.status.success() {
                    // stderr already printed by run_command
                    return Err(Box::new(io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "'{} listremotes' failed. Cannot continue.",
                            rclone_executable
                        ),
                    )));
                }
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(|line| line.trim())
                    .filter(|line| !line.is_empty() && line.ends_with(':')) // Filter for lines ending with ':'
                    .map(|line| line.trim_end_matches(':').to_string()) // Remove trailing ':' and collect
                    .collect()
            }
            Err(e) => {
                eprintln!("Error running '{} listremotes': {}", rclone_executable, e);
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
    println!("Processing remotes in parallel (running lsjson)...");
    let loop_start_time = Instant::now();

    // Use rayon's par_iter to map over remote names in parallel.
    // Collect results into a Vec.
    let results: Vec<RemoteProcessingResult> = remote_names
        .par_iter() // Create parallel iterator
        .map(|remote_name| {
            // This closure runs potentially in parallel for each remote_name
            println!("  Starting lsjson for: {}", remote_name);
            let start = Instant::now();

            // Clone common args (which are Vec<String>) for this thread/task
            let mut lsjson_args_owned: Vec<String> = common_rclone_args.clone();

            // Format the remote target including the colon
            let remote_target = format!("{}:", remote_name);

            // Extend the Vec<String> with more Strings
            lsjson_args_owned.extend(vec![
                "lsjson".to_string(),
                "-R".to_string(),
                "--hash".to_string(),
                "--fast-list".to_string(),
                remote_target, // Use "remote:" format
            ]);

            // Convert Vec<String> to Vec<&str> *just* for the run_command call
            let lsjson_args_for_cmd: Vec<&str> = lsjson_args_owned
                .iter()
                .map(|s| s.as_str()) // Convert String to &str
                .collect();

            // Execute the command using run_command from lib.rs
            match cloudmapper::run_command(rclone_executable, &lsjson_args_for_cmd) {
                Ok(output) => {
                    if output.status.success() {
                        let json_string = String::from_utf8_lossy(&output.stdout);
                        // Parse JSON
                        match cloudmapper::parse_rclone_lsjson(&json_string) {
                            Ok(raw_files) => {
                                let duration = start.elapsed();
                                println!(
                                    "  Finished lsjson for {} ({} items) in {:.2}s",
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
                                    "Error parsing lsjson JSON for remote '{}': {}",
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
                            "rclone lsjson command failed for remote '{}'", // Status logged by run_command
                            remote_name                                     //, output.status
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
                        "Error executing rclone lsjson command for remote '{}': {}",
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
        "Finished parallel lsjson processing phase in {:.2}s.",
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
    let mut report_generation_error = false;
    if files_collection.files.is_empty() {
        if process_errors > 0 {
            println!(
                "No file data was collected due to processing errors. Skipping standard report generation."
            );
        } else if remotes_processed_successfully == 0 && !remote_names.is_empty() {
            // This case might mean listremotes worked but all lsjson calls failed before parsing
            println!("No data successfully processed from any remote. Skipping standard report generation.");
        } else {
            // This means lsjson ran successfully but found 0 files/dirs on any remote
            println!("No files found across any successfully processed remotes. Skipping standard report generation.");
        }
        // Still allow 'about' report to run even if no files found, as long as remotes exist
        if !args.about_report || remote_names.is_empty() {
            println!("No reports to generate. Exiting.");
            return Ok(()); // Exit if no files and no about report requested/possible
        } else {
            println!("Proceeding with 'about' report generation only.");
            // Fall through to 'about' report generation below
        }
    } else {
        // --- Generate Standard Reports (Only if files_collection is not empty) ---
        println!("Generating standard reports (tree/files, size_used, duplicates)...");
        let report_start_time = Instant::now();

        // Convert the CLI enum variant to the lib's enum variant
        let output_division_mode_lib: OutputDivisionMode = args.output_division.into();

        match cloudmapper::generate_reports(
            &mut files_collection,
            output_division_mode_lib, // Pass chosen division mode
            args.duplicates,
            &output_dir,           // Pass the output directory PathBuf
            TREE_OUTPUT_FILE_NAME, // Pass base name for Single mode
            TREE_OUTPUT_FILE_NAME, // Pass base name for Folder mode content files
            DUPLICATES_OUTPUT_FILE_NAME,
            SIZE_OUTPUT_FILE_NAME,
            FOLDER_ICON,
            FILE_ICON,
            SIZE_ICON,
            DATE_ICON,
            REMOTE_ICON,
        ) {
            Ok(_) => {
                println!(
                    "Standard reports generated successfully in {:.2}s.",
                    report_start_time.elapsed().as_secs_f32()
                );
                // Adjust success message based on mode
                match output_division_mode_lib {
                    OutputDivisionMode::Single => {
                        println!(
                            "  File list report: {}",
                            output_dir.join(TREE_OUTPUT_FILE_NAME).display()
                        );
                    }
                    OutputDivisionMode::Remote | OutputDivisionMode::Folder => {
                        println!(
                            "  File list/structure output generated in: {}",
                            output_dir.display()
                        );
                    }
                }
                println!(
                    "  Size report: {}",
                    output_dir.join(SIZE_OUTPUT_FILE_NAME).display()
                );
                if args.duplicates {
                    println!(
                        "  Duplicates report: {}",
                        output_dir.join(DUPLICATES_OUTPUT_FILE_NAME).display()
                    );
                }
            }
            Err(e) => {
                eprintln!("Error generating standard reports: {}", e);
                report_generation_error = true; // Mark error
            }
        }
    }

    // --- Generate About Report (if enabled) ---
    let mut about_report_error = false;
    if args.about_report {
        let about_report_start_time = Instant::now();
        // Construct full path for the about report
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        // Get the list of service names that were actually processed (from roots_by_service keys)
        // or fall back to the original list if files_collection is empty but remotes were found
        let services_to_query = if !files_collection.roots_by_service.is_empty() {
            files_collection.get_service_names()
        } else {
            remote_names.clone() // Use the initial list if no files were processed
        };

        if !services_to_query.is_empty() {
            match cloudmapper::generate_about_report(
                &services_to_query, // Pass the list of services/remotes
                rclone_executable,
                &common_rclone_args, // Pass common args (like --config)
                // Pass path as &str
                about_output_path.to_str().unwrap_or_else(|| {
                    eprintln!("Warning: Could not convert about output path to string. Skipping about report.");
                    "" // Provide an empty string to cause generate_about_report to error safely
                }),
            ) {
                Ok(_) => {
                    println!(
                        "'About' report generated successfully in {:.2}s.",
                        about_report_start_time.elapsed().as_secs_f32()
                    );
                    println!("  About report: {}", about_output_path.display());
                }
                Err(e) => {
                    eprintln!("Error generating 'about' report: {}", e);
                    about_report_error = true; // Mark error
                }
            }
        } else {
            println!("Skipping 'about' report as no remotes were identified for querying.");
            // Optionally clean up old report if exists and skipping
            if about_output_path.exists() {
                let _ = fs::remove_file(&about_output_path);
                // println!("Removed existing about report file '{}'", about_output_path.display());
            }
        }
    } else {
        println!("'About' report generation skipped by flag.");
        // Optionally clean up old report if exists and skipping
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        if about_output_path.exists() {
            let _ = fs::remove_file(&about_output_path);
            // println!("Removed existing about report file '{}'", about_output_path.display());
        }
    }

    // --- 5. Print Summary ---
    let total_duration = overall_start_time.elapsed();
    println!("==========================================");
    println!("Processing Summary:");
    println!("  Total time: {:.2}s", total_duration.as_secs_f32());
    println!("  Remotes found: {}", remote_names.len());
    println!(
        "  Remotes processed successfully (lsjson): {}",
        remotes_processed_successfully
    );
    // Consolidate error reporting
    let total_errors = process_errors
        + if report_generation_error { 1 } else { 0 }
        + if about_report_error { 1 } else { 0 };
    println!(
        "  Processing/Reporting errors encountered: {}",
        total_errors
    );
    println!("==========================================");

    if total_errors > 0 {
        eprintln!("Completed with errors.");
        // Decide if any error should result in a non-zero exit code
        // Let's return an error if lsjson processing had errors or report generation failed
        if process_errors > 0 || report_generation_error || about_report_error {
            return Err(Box::new(io::Error::new(
                ErrorKind::Other,
                "Processing completed with errors",
            )));
        }
    }

    println!("Processing completed successfully.");
    Ok(())
}
