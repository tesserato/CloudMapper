//! CloudMapper executable entry point.
//!
//! This binary uses the `cloudmapper` library to:
//! 1. Parse command-line arguments.
//! 2. Interact with `rclone` to list remotes, get storage usage (`about`), and list files (`lsjson`).
//! 3. Process the collected data.
//! 4. Generate various reports (tree structure, duplicates, extensions, size, about, HTML treemap)
//!    based on user configuration.
//! 5. Write the reports to the specified output directory.

use clap::Parser;
use rayon::prelude::*; // Import Rayon traits for parallel iterators

use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::Instant;

// Import types and functions from the library crate
use cloudmapper::{AboutResult, Files, OutputMode, RawFile, RcloneAboutInfo};

// --- Configuration Constants (Base Filenames) ---
/// Base filename for the main file list/tree output.
/// Used directly in `Single` mode and as the content filename within directories in `Folders` mode.
const TREE_OUTPUT_FILE_NAME: &str = "files.txt";
/// Filename for the duplicate files report.
const DUPLICATES_OUTPUT_FILE_NAME: &str = "duplicates.txt";
/// Filename for the report summarizing calculated size usage per remote and total.
const SIZE_OUTPUT_FILE_NAME: &str = "size_used.txt";
/// Filename for the report summarizing `rclone about` information.
const ABOUT_OUTPUT_FILE_NAME: &str = "about.txt";
/// Filename for the report summarizing file counts and sizes per extension.
const EXTENSIONS_OUTPUT_FILE_NAME: &str = "extensions.txt";
/// Filename for the HTML treemap visualization report.
const TREEMAP_OUTPUT_FILE_NAME: &str = "treemap.html";
/// Filename for the largest files report.
const LARGEST_FILES_OUTPUT_FILE_NAME: &str = "largest_files.txt";

// --- Icons Used in Text Reports ---
/// Icon representing a cloud remote.
const REMOTE_ICON: &str = "☁️";
/// Icon representing a folder/directory.
const FOLDER_ICON: &str = "📁";
/// Icon representing a file.
const FILE_ICON: &str = "📄";
/// Icon representing size information.
const SIZE_ICON: &str = "💽";
/// Icon representing date/time information.
const DATE_ICON: &str = "📆";

/// Separator string used for console output sections.
const SECTION_SEPARATOR: &str =
    "==========================================================================================";

// --- Command Line Argument Definition ---
/// Defines the command-line arguments accepted by the CloudMapper application,
/// powered by the `clap` crate.
#[derive(Parser, Debug)]
#[command(version, about = "Analyzes cloud storage using rclone and generates reports.", long_about = None)]
struct Args {
    /// Path to a specific rclone executable (optional).
    /// If not provided, 'rclone' from the system's PATH environment variable will be used.
    /// Can also be set via the `RCLONE_EXECUTABLE` environment variable.
    #[arg(long, short = 'r', env = "RCLONE_EXECUTABLE")]
    rclone_path: Option<String>,

    /// Path to a specific rclone configuration file (optional).
    /// If not provided, rclone will use its default configuration file location.
    /// Can also be set via the `RCLONE_CONFIG` environment variable.
    #[arg(long, short = 'c', env = "RCLONE_CONFIG")]
    rclone_config: Option<String>,

    /// Path to the directory where the generated reports will be saved.
    /// Defaults to './cloud'.
    /// Can also be set via the `CM_OUTPUT` environment variable.
    #[arg(long, short = 'o', env = "CM_OUTPUT", default_value = "./cloud")]
    output_path: String,

    /// Specifies how the main file list/tree report output should be structured.
    /// Options: 'single' (one file), 'remotes' (one file per remote), 'folders' (directory structure).
    /// Defaults to 'folders'.
    /// Can also be set via the `CM_OUTPUT_MODE` environment variable.
    #[arg(
        long,
        short = 'm',
        value_enum, // Use clap's value_enum feature for OutputMode
        default_value_t = OutputMode::Folders, // Default to folder structure
        env = "CM_OUTPUT_MODE"
    )]
    output_mode: OutputMode,

    /// Enable the duplicate file detection report (`duplicates.txt`).
    /// Defaults to true. Set to `false` to disable.
    /// Can also be set via the `CM_DUPLICATES` environment variable (e.g., `CM_DUPLICATES=false`).
    #[arg(long, short = 'd', default_value_t = true, env = "CM_DUPLICATES")]
    duplicates: bool,

    /// Enable the file extensions report (`extensions.txt`).
    /// Defaults to true. Set to `false` to disable.
    /// Can also be set via the `CM_EXTENSIONS` environment variable.
    #[arg(long, short = 'e', default_value_t = true, env = "CM_EXTENSIONS")]
    extensions_report: bool,

    /// Number of largest files to list in the 'largest_files.txt' report.
    /// Set to 0 to disable this report. Defaults to 100.
    /// Can also be set via the `CM_LARGEST_FILES` environment variable.
    #[arg(long, short = 'l', default_value_t = 100, env = "CM_LARGEST_FILES")]
    largest_files: usize,

    /// Enable the 'rclone about' report (`about.txt`) for remote storage usage summary.
    /// Defaults to true. Set to `false` to disable.
    /// Can also be set via the `CM_ABOUT` environment variable.
    #[arg(long, short = 'a', default_value_t = true, env = "CM_ABOUT")]
    about_report: bool,

    /// Enable the HTML treemap visualization report (`treemap.html`).
    /// Defaults to true. Set to `false` to disable.
    /// Can also be set via the `CM_HTML_TREEMAP` environment variable.
    #[arg(long, short = 't', default_value_t = true, env = "CM_HTML_TREEMAP")]
    html_treemap: bool,

    /// Clean (remove) the output directory before generating new reports.
    /// Defaults to true. Set to `false` to keep existing files (new reports may overwrite).
    /// Can also be set via the `CM_CLEAN_OUTPUT` environment variable.
    #[arg(long, short = 'k', default_value_t = true, env = "CM_CLEAN_OUTPUT")]
    clean_output: bool,
}

/// Type alias for the result of processing a single remote with the `rclone lsjson` command.
/// Contains either the remote name and its parsed `RawFile` list, or the remote name and an error message.
type RemoteProcessingResult = Result<(String, Vec<RawFile>), (String, String)>;

/// Main application entry point.
/// Parses arguments, runs rclone commands, processes data, and generates reports.
///
/// # Returns
/// * `Ok(())` on successful completion of all tasks.
/// * `Err(Box<dyn std::error::Error>)` if any critical step fails (e.g., listing remotes,
///   generating essential reports, filesystem errors). Errors during individual remote
///   processing (`lsjson` or `about`) are logged but may not cause the entire program to fail.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Parse Command Line Arguments ---
    let args = Args::parse();

    // --- Determine rclone executable path ---
    // Use provided path from args, fallback to env var (handled by clap), or default to "rclone".
    let rclone_executable = args.rclone_path.as_deref().unwrap_or("rclone");

    // --- Print Configuration & Start ---
    println!("Starting CloudMapper...");
    println!("{SECTION_SEPARATOR}");
    println!("Configuration:");
    println!("  Rclone Executable: {}", rclone_executable);
    if let Some(conf) = &args.rclone_config {
        println!("  Rclone Config: {}", conf);
    } else {
        println!("  Rclone Config: Using default location");
    }
    println!("  Output Directory: {}", args.output_path);
    println!("  Output Mode: {:?}", args.output_mode);
    println!("  Clean Output Dir: {}", args.clean_output);
    println!("  Duplicates Report: {}", args.duplicates);
    println!("  Extensions Report: {}", args.extensions_report);
    println!("  Largest Files Report (Top N): {}", args.largest_files);
    println!("  About Report: {}", args.about_report);
    println!("  HTML Treemap Report: {}", args.html_treemap);
    println!("{SECTION_SEPARATOR}");

    println!("Initializing...");
    let overall_start_time = Instant::now();

    // --- Create/Clean Output Directory ---
    let output_dir = PathBuf::from(&args.output_path);
    if args.clean_output && output_dir.exists() {
        println!("Cleaning output directory '{}'...", output_dir.display());
        // Attempt to remove the directory and its contents. Propagate error if it fails.
        fs::remove_dir_all(&output_dir)?;
        println!("Output directory cleaned.");
    }
    // Ensure the output directory exists, creating it if necessary. Propagate error if it fails.
    fs::create_dir_all(&output_dir)?;
    println!("Output directory '{}' ensured.", output_dir.display());
    println!("{SECTION_SEPARATOR}");

    // Initialize the main data structure from the library.
    let mut files_collection = Files::new();

    // --- 1. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");
    // Prepare common rclone arguments (like --config) used in multiple commands.
    let mut common_rclone_args: Vec<String> = Vec::new();
    if let Some(config_path) = &args.rclone_config {
        common_rclone_args.push("--config".to_string());
        common_rclone_args.push(config_path.clone());
    }
    // Create a slice of &str from common_args for the run_command function.
    let mut list_remotes_args_for_cmd: Vec<&str> =
        common_rclone_args.iter().map(|s| s.as_str()).collect();
    // Add the specific command 'listremotes'.
    list_remotes_args_for_cmd.push("listremotes");

    // Execute `rclone listremotes`.
    let remote_names: Vec<String> =
        match cloudmapper::run_command(rclone_executable, &list_remotes_args_for_cmd) {
            Ok(output) => {
                // Check if the command executed successfully (exit code 0).
                if !output.status.success() {
                    // stderr already printed by run_command if it failed
                    eprintln!(
                        "Error: '{} listremotes' command failed with status {}. Cannot continue.",
                        rclone_executable, output.status
                    );
                    // Return a specific error indicating failure to list remotes.
                    return Err(Box::new(io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "'{} listremotes' failed. Check rclone configuration and connectivity.",
                            rclone_executable
                        ),
                    )));
                }
                // Parse the stdout to get remote names.
                String::from_utf8_lossy(&output.stdout)
                    .lines() // Split into lines
                    .map(|line| line.trim()) // Trim whitespace
                    .filter(|line| !line.is_empty() && line.ends_with(':')) // Filter for lines ending with ':'
                    .map(|line| line.trim_end_matches(':').to_string()) // Remove trailing ':' and convert to String
                    .collect() // Collect into a Vec<String>
            }
            Err(e) => {
                // Handle errors during command execution (e.g., rclone not found).
                eprintln!(
                    "Fatal Error: Failed to execute '{} listremotes': {}",
                    rclone_executable, e
                );
                return Err(Box::new(e)); // Propagate the underlying IO error.
            }
        };

    // Check if any remotes were found.
    if remote_names.is_empty() {
        println!("Warning: No rclone remotes found or 'rclone listremotes' returned empty. No data to process. Exiting.");
        // Write empty reports or cleanup output dir? For now, just exit cleanly.
        return Ok(());
    }
    println!("Found {} remotes: {:?}", remote_names.len(), remote_names);
    println!("{SECTION_SEPARATOR}");

    // --- 2. Generate About Report (Parallel Fetch, Sequential Process) ---
    let mut about_report_error = false; // Flag for errors specific to the 'about' report phase.
    if args.about_report {
        println!("Fetching 'rclone about' info in parallel...");
        let about_fetch_start_time = Instant::now();
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);

        // Determine which services to query for 'about' info.
        // At this stage, files_collection is empty, so we must use remote_names.
        let services_to_query = remote_names.clone();

        // Proceed only if there are services to query.
        if !services_to_query.is_empty() {
            // Fetch 'about --json' for each service in parallel using Rayon.
            let about_results: Vec<AboutResult> = services_to_query
                .par_iter()
                .map(|remote_name| {
                    // Prepare arguments for `rclone about <remote>: --json`
                    let remote_target = format!("{}:", remote_name);
                    let mut about_args_owned: Vec<String> = common_rclone_args.clone();
                    about_args_owned.extend(vec![
                        "about".to_string(),
                        remote_target, // Target remote
                        "--json".to_string(), // Request JSON output
                    ]);
                    let about_args_for_cmd: Vec<&str> =
                        about_args_owned.iter().map(|s| s.as_str()).collect();

                    // Execute the command for this remote.
                    match cloudmapper::run_command(rclone_executable, &about_args_for_cmd) {
                        Ok(output) => {
                            // Command executed, check status.
                            if output.status.success() {
                                // Command succeeded, parse JSON.
                                let json_string = String::from_utf8_lossy(&output.stdout);
                                match serde_json::from_str::<RcloneAboutInfo>(&json_string) {
                                    Ok(info) => Ok((remote_name.clone(), info)), // Success
                                    Err(e) => {
                                        // JSON parsing failed.
                                        let err_msg =
                                            format!("Failed to parse 'about' JSON for remote '{}': {}", remote_name, e);
                                        eprintln!("  Warning: {}", err_msg); // Log warning
                                        // The suggestion to reconnect is not strictly for parsing errors, but for command failures.
                                        Err((remote_name.clone(), err_msg)) // Return error
                                    }
                                }
                            } else {
                                // rclone about command failed.
                                let err_msg = format!(
                                    "'about' command failed for remote '{}' (Status {}). Check logs for stderr.",
                                    remote_name,
                                    output.status,
                                );
                                eprintln!("  Warning: {}", err_msg); // Log warning
                                eprintln!("  Suggestion: Try reconnecting the remote with 'rclone config reconnect {}:'", remote_name);
                                Err((remote_name.clone(), err_msg)) // Return error
                            }
                        }
                        Err(e) => {
                            // Failed to execute rclone about.
                            let err_msg = format!("Failed to execute 'about' command for remote '{}': {}", remote_name, e);
                            eprintln!("  Warning: {}", err_msg); // Log warning
                            eprintln!("  Suggestion: Try reconnecting the remote with 'rclone config reconnect {}:'", remote_name);
                            Err((remote_name.clone(), err_msg)) // Return error
                        }
                    }
                })
                .collect(); // Collect results from parallel tasks.

            let about_fetch_duration = about_fetch_start_time.elapsed();
            println!(
                "Finished parallel 'about' fetching in {:.2}s.",
                about_fetch_duration.as_secs_f32()
            );

            // Process the collected 'about' results sequentially and write the report.
            match cloudmapper::process_about_results(
                about_results,
                about_output_path.to_str().unwrap_or_else(|| {
                    // Handle unlikely case where output path isn't valid UTF-8.
                    eprintln!(
                        "Warning: Could not convert 'about' output path to string. Using default filename '{}'.",
                         ABOUT_OUTPUT_FILE_NAME
                    );
                    ABOUT_OUTPUT_FILE_NAME // Use default name if path conversion fails
                }),
            ) {
                Ok(_) => { /* Success message already printed by process_about_results */ }
                Err(e) => {
                    // Error occurred during result processing or file writing.
                    eprintln!("Error processing 'about' results or writing report: {}", e);
                    about_report_error = true; // Set flag
                }
            }
        } else {
            // No services were identified to query (e.g., listremotes failed or returned empty).
            println!("Skipping 'about' report as no remotes were identified for querying.");
            // Ensure no old 'about' report file remains if skipped.
            if about_output_path.exists() {
                let _ = fs::remove_file(&about_output_path);
            }
        }
    } else {
        // 'About' report was explicitly disabled by flag.
        println!("'About' report generation skipped by flag.");
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        // Ensure no old 'about' report file remains if skipped.
        if about_output_path.exists() {
            let _ = fs::remove_file(&about_output_path);
        }
    }
    println!("{SECTION_SEPARATOR}");

    // --- 3. Process Remotes (lsjson) in Parallel ---
    println!("Processing remotes in parallel (rclone lsjson)...");
    let lsjson_start_time = Instant::now();

    // Use Rayon's `par_iter` to process each remote concurrently.
    let lsjson_results: Vec<RemoteProcessingResult> = remote_names
        .par_iter() // Create a parallel iterator over remote names.
        .map(|remote_name| { // Map each remote name to a processing result.
            // This closure runs in parallel for each remote.
            // println!("  Starting lsjson for: {}", remote_name); // Reduced console noise

            let start = Instant::now(); // Time individual remote processing.
            // Prepare arguments specific to this remote's lsjson command.
            let mut lsjson_args_owned: Vec<String> = common_rclone_args.clone(); // Start with common args
            let remote_target = format!("{}:", remote_name); // Format as "remote:"
            lsjson_args_owned.extend(vec![
                "lsjson".to_string(), // Command
                "-R".to_string(),     // Recursive flag
                "--hash".to_string(), // Request hashes
                "--fast-list".to_string(), // Use fast-list option if supported by remote
                remote_target,        // The remote to list
            ]);
            // Convert owned Strings back to &str slice for run_command.
            let lsjson_args_for_cmd: Vec<&str> =
                lsjson_args_owned.iter().map(|s| s.as_str()).collect();

            // Execute the rclone lsjson command for this remote.
            match cloudmapper::run_command(rclone_executable, &lsjson_args_for_cmd) {
                Ok(output) => {
                    // Command executed, check status code.
                    if output.status.success() {
                        // Command succeeded, get stdout.
                        let json_string = String::from_utf8_lossy(&output.stdout);
                        // Attempt to parse the JSON output.
                        match cloudmapper::parse_rclone_lsjson(&json_string) {
                            Ok(raw_files) => {
                                // Successfully parsed JSON.
                                let duration = start.elapsed();
                                println!(
                                    "  Finished lsjson for {} ({} items) in {:.2}s",
                                    remote_name,
                                    raw_files.len(),
                                    duration.as_secs_f32()
                                );
                                // Return Ok result with remote name and parsed files.
                                Ok((remote_name.clone(), raw_files))
                            }
                            Err(e) => {
                                // JSON parsing failed.
                                let err_msg = format!(
                                    "Error parsing lsjson JSON for remote '{}': {}. Rclone output might be incomplete or invalid.",
                                    remote_name, e
                                );
                                eprintln!("  {}", err_msg); // Log the parsing error.
                                // Return Err result with remote name and error message.
                                Err((remote_name.clone(), err_msg))
                            }
                        }
                    } else {
                        // rclone lsjson command failed (non-zero exit code).
                         // stderr likely already printed by run_command, create concise message here.
                        let err_msg = format!(
                            "rclone lsjson command failed for remote '{}'. Status: {}. Check logs for stderr.",
                            remote_name,
                            output.status,
                        );
                        eprintln!("  {}", err_msg); // Log the command failure.
                        // Return Err result with remote name and error message.
                        Err((remote_name.clone(), err_msg))
                    }
                }
                Err(e) => {
                    // Failed to execute the rclone command itself.
                    let err_msg = format!(
                        "Error executing rclone lsjson command for remote '{}': {}. Check rclone path and permissions.",
                        remote_name, e
                    );
                     // Error likely already printed by run_command.
                    eprintln!("  {}", err_msg); // Log the execution failure.
                    // Return Err result with remote name and error message.
                    Err((remote_name.clone(), err_msg))
                }
            }
        })
        .collect(); // Collect results from all parallel tasks into the lsjson_results Vec.

    let lsjson_duration = lsjson_start_time.elapsed();
    println!(
        "Finished parallel lsjson processing phase in {:.2}s.",
        lsjson_duration.as_secs_f32()
    );
    println!("{SECTION_SEPARATOR}");

    // --- 4. Aggregate lsjson Results Sequentially ---
    println!("Aggregating lsjson results...");
    let mut lsjson_process_errors: u32 = 0; // Count errors during lsjson phase.
    let mut remotes_processed_successfully: u32 = 0; // Count successful lsjson runs.
    for result in lsjson_results {
        match result {
            Ok((remote_name, raw_files)) => {
                // Add the successfully parsed files to the main collection.
                files_collection.add_remote_files(&remote_name, raw_files);
                remotes_processed_successfully += 1;
            }
            Err(_) => {
                // An error occurred for this remote (already logged). Increment error count.
                lsjson_process_errors += 1;
            }
        }
    }
    println!(
        "Aggregation complete. {} remotes processed successfully, {} errors encountered.",
        remotes_processed_successfully, lsjson_process_errors
    );
    println!("{SECTION_SEPARATOR}");

    // --- 5. Generate Standard Reports (if data exists) ---
    let mut report_generation_error = false; // Flag to track errors during report generation.

    // Check if any file data was actually collected before attempting reports.
    if files_collection.files.is_empty() {
        if lsjson_process_errors > 0 {
            println!(
                "Warning: No file data collected from 'lsjson' due to {} error(s). Skipping standard reports (Tree, Size, Duplicates, Extensions, Largest Files, Treemap).",
                lsjson_process_errors
            );
        } else if remotes_processed_successfully == 0 && !remote_names.is_empty() {
             println!("Warning: No data successfully processed from any remote via 'lsjson'. Skipping standard reports.");
        } else if !remote_names.is_empty() { // implies remotes_processed_successfully > 0 but 0 files found
             println!("No files found across successfully processed remotes via 'lsjson'. Skipping standard reports.");
        } else { // This case should ideally be caught by the remote_names.is_empty() check after listremotes
             println!("No remotes available. Skipping standard reports.");
        }
        // The 'about' report has already been handled or skipped by this point.
    } else {
        // We have file data, proceed with generating standard reports.
        println!("Generating standard reports...");
        let report_start_time = Instant::now();

        // Call the library function to generate reports based on collected data and flags.
        match cloudmapper::generate_reports(
            &mut files_collection, // Pass mutable reference to the data collection
            args.output_mode,
            args.duplicates,
            args.extensions_report,
            args.largest_files, 
            args.html_treemap,
            &output_dir,
            TREE_OUTPUT_FILE_NAME,
            TREE_OUTPUT_FILE_NAME, // folder_content_filename, same as tree_base_filename for now
            DUPLICATES_OUTPUT_FILE_NAME,
            SIZE_OUTPUT_FILE_NAME,
            EXTENSIONS_OUTPUT_FILE_NAME,
            LARGEST_FILES_OUTPUT_FILE_NAME, 
            TREEMAP_OUTPUT_FILE_NAME,
            FOLDER_ICON,
            FILE_ICON,
            SIZE_ICON,
            DATE_ICON,
            REMOTE_ICON,
        ) {
            Ok(_) => {
                // Reports generated successfully.
                println!(
                    "Standard reports generated successfully in {:.2}s.",
                    report_start_time.elapsed().as_secs_f32()
                );
                // Print paths to the main generated files for user convenience.
                match args.output_mode {
                    OutputMode::Single => println!(
                        "  File list report: {}",
                        output_dir.join(TREE_OUTPUT_FILE_NAME).display()
                    ),
                    _ => println!(
                        "  File list/structure output generated in: {}",
                        output_dir.display()
                    ),
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
                if args.extensions_report {
                    println!(
                        "  Extensions report: {}",
                        output_dir.join(EXTENSIONS_OUTPUT_FILE_NAME).display()
                    );
                }
                if args.largest_files > 0 {
                    println!(
                        "  Largest Files report: {}",
                        output_dir.join(LARGEST_FILES_OUTPUT_FILE_NAME).display()
                    );
                }
                if args.html_treemap {
                    println!(
                        "  HTML Treemap report: {}",
                        output_dir.join(TREEMAP_OUTPUT_FILE_NAME).display()
                    );
                }
            }
            Err(e) => {
                // An error occurred during report generation (e.g., file write error).
                eprintln!("Error generating standard reports: {}", e);
                report_generation_error = true; // Set flag to indicate failure.
            }
        }
    }
    println!("{SECTION_SEPARATOR}");


    // --- 6. Print Summary ---
    let total_duration = overall_start_time.elapsed();
    println!("{SECTION_SEPARATOR}");
    println!("Processing Summary:");
    println!(
        "  Total execution time: {:.2}s",
        total_duration.as_secs_f32()
    );
    println!("  Remotes found via 'listremotes': {}", remote_names.len());
    println!(
        "  Remotes successfully processed via 'lsjson': {}",
        remotes_processed_successfully
    );
    let total_errors = lsjson_process_errors 
        + if report_generation_error { 1 } else { 0 } 
        + if about_report_error { 1 } else { 0 }; 
    println!(
        "  Errors encountered during processing/reporting: {}",
        total_errors
    );
    println!("{SECTION_SEPARATOR}");

    // --- 7. Final Result ---
    if total_errors > 0 || report_generation_error || about_report_error {
        eprintln!("CloudMapper completed with errors. Please check logs above.");
        Err(Box::new(io::Error::new(
            ErrorKind::Other,
            "Processing completed with one or more errors.",
        )))
    } else {
        println!("CloudMapper processing completed successfully.");
        Ok(())
    }
}