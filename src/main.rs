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
use std::io::{self, ErrorKind, Write}; // Added Write for flush
use std::path::PathBuf;
use std::process::Command; // For running reconnect
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

/// Helper function to ask a yes/no question to the user.
fn ask_yes_no(prompt: &str) -> bool {
    let mut input = String::new();
    print!("{}", prompt);
    let _ = io::stdout().flush(); // Ensure the prompt is displayed before reading input
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    let response = input.trim().to_lowercase();
    response == "y" || response == "yes"
}

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
        fs::remove_dir_all(&output_dir)?;
        println!("Output directory cleaned.");
    }
    fs::create_dir_all(&output_dir)?;
    println!("Output directory '{}' ensured.", output_dir.display());
    println!("{SECTION_SEPARATOR}");

    let mut files_collection = Files::new();

    // --- 1. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");
    let mut common_rclone_args: Vec<String> = Vec::new();
    if let Some(config_path) = &args.rclone_config {
        common_rclone_args.push("--config".to_string());
        common_rclone_args.push(config_path.clone());
    }
    let mut list_remotes_args_for_cmd: Vec<&str> =
        common_rclone_args.iter().map(|s| s.as_str()).collect();
    list_remotes_args_for_cmd.push("listremotes");

    let remote_names: Vec<String> =
        match cloudmapper::run_command(rclone_executable, &list_remotes_args_for_cmd) {
            Ok(output) => {
                if !output.status.success() {
                    eprintln!(
                        "Error: '{} listremotes' command failed with status {}. Cannot continue.",
                        rclone_executable, output.status
                    );
                    return Err(Box::new(io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "'{} listremotes' failed. Check rclone configuration and connectivity.",
                            rclone_executable
                        ),
                    )));
                }
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .map(|line| line.trim())
                    .filter(|line| !line.is_empty() && line.ends_with(':'))
                    .map(|line| line.trim_end_matches(':').to_string())
                    .collect()
            }
            Err(e) => {
                eprintln!(
                    "Fatal Error: Failed to execute '{} listremotes': {}",
                    rclone_executable, e
                );
                return Err(Box::new(e));
            }
        };

    if remote_names.is_empty() {
        println!("Warning: No rclone remotes found. No data to process. Exiting.");
        return Ok(());
    }
    println!("Found {} remotes: {:?}", remote_names.len(), remote_names);
    println!("{SECTION_SEPARATOR}");

    // --- 2. Generate About Report (Parallel Fetch, then Interactive Processing for Failures) ---
    let mut about_report_error_flag = false;
    let mut remotes_for_lsjson_processing = remote_names.clone(); // Start with all, will be filtered
    let mut final_about_results_for_report: Vec<AboutResult> = Vec::new();

    if args.about_report {
        println!("Fetching 'rclone about' info in parallel...");
        let about_fetch_start_time = Instant::now();

        let initial_about_results: Vec<AboutResult> = remote_names
            .par_iter()
            .map(|remote_name_str| {
                let remote_name = remote_name_str.clone();
                let remote_target = format!("{}:", remote_name);
                let mut about_args_owned: Vec<String> = common_rclone_args.clone();
                about_args_owned.extend(vec![
                    "about".to_string(),
                    remote_target.clone(),
                    "--json".to_string(),
                ]);
                let about_args_for_cmd: Vec<&str> =
                    about_args_owned.iter().map(|s| s.as_str()).collect();

                match cloudmapper::run_command(rclone_executable, &about_args_for_cmd) {
                    Ok(output) => {
                        if output.status.success() {
                            let json_string = String::from_utf8_lossy(&output.stdout);
                            match serde_json::from_str::<RcloneAboutInfo>(&json_string) {
                                Ok(info) => Ok((remote_name, info)),
                                Err(e) => {
                                    let err_msg = format!("Failed to parse 'about' JSON for remote '{}': {}", remote_name, e);
                                    Err((remote_name, err_msg))
                                }
                            }
                        } else {
                            let err_msg = format!("'about' command failed for remote '{}' (Status {}).", remote_name, output.status);
                            Err((remote_name, err_msg))
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("Failed to execute 'about' command for remote '{}': {}", remote_name, e);
                        Err((remote_name, err_msg))
                    }
                }
            })
            .collect();

        let about_fetch_duration = about_fetch_start_time.elapsed();
        println!(
            "Finished parallel 'about' fetching in {:.2}s.",
            about_fetch_duration.as_secs_f32()
        );
        
        if !initial_about_results.is_empty() {
             println!("Interactively processing 'about' failures (if any)...");
        }

        // This loop must be sequential due to user interaction and potential re-runs
        for result in initial_about_results {
            match result {
                Ok(success_result) => {
                    final_about_results_for_report.push(Ok(success_result));
                }
                Err((remote_name, original_err_msg)) => { // remote_name is owned String here
                    eprintln!("  Warning: Initial 'rclone about' failed for remote '{}': {}", remote_name, original_err_msg);
                    let prompt_str = format!("  Do you want to attempt to reconnect remote '{}' and retry 'about'? (y/N): ", remote_name);
                    
                    if ask_yes_no(&prompt_str) {
                        println!("  Attempting to reconnect remote '{}'...", remote_name);
                        let reconnect_target_str = format!("{}:", remote_name); // Create a &str compatible string
                        let mut reconnect_args_cli: Vec<&str> = common_rclone_args.iter().map(|s| s.as_str()).collect();
                        reconnect_args_cli.push("config");
                        reconnect_args_cli.push("reconnect");
                        reconnect_args_cli.push(&reconnect_target_str); // Pass as reference

                        let mut cmd = Command::new(rclone_executable);
                        cmd.args(&reconnect_args_cli);
                        println!("    Running: {} {}", rclone_executable, reconnect_args_cli.join(" "));

                        match cmd.status() {
                            Ok(status) => {
                                if status.success() {
                                    println!("    Reconnect command for '{}' completed successfully.", remote_name);
                                } else {
                                    println!("    Reconnect command for '{}' failed. Status: {}.", remote_name, status);
                                }
                            }
                            Err(e) => {
                                println!("    Failed to execute reconnect command for '{}': {}.", remote_name, e);
                            }
                        }

                        // --- RETRY 'ABOUT' for this remote ---
                        println!("  Retrying 'rclone about' for '{}'...", remote_name);
                        let retry_remote_target_str = format!("{}:", remote_name);
                        let mut retry_about_args_owned: Vec<String> = common_rclone_args.clone();
                        retry_about_args_owned.extend(vec![
                            "about".to_string(),
                            retry_remote_target_str.clone(), 
                            "--json".to_string(),
                        ]);
                        let retry_about_args_for_cmd: Vec<&str> =
                            retry_about_args_owned.iter().map(|s| s.as_str()).collect();

                        let retry_about_result = match cloudmapper::run_command(rclone_executable, &retry_about_args_for_cmd) {
                            Ok(output) => {
                                if output.status.success() {
                                    let json_string = String::from_utf8_lossy(&output.stdout);
                                    match serde_json::from_str::<RcloneAboutInfo>(&json_string) {
                                        Ok(info) => {
                                            println!("    Retry 'rclone about' for '{}' SUCCEEDED.", remote_name);
                                            Ok((remote_name.clone(), info)) 
                                        },
                                        Err(e) => {
                                            let err_msg = format!("Failed to parse 'about' JSON for remote '{}' on retry: {}", remote_name, e);
                                            eprintln!("    Warning: {}", err_msg);
                                            Err((remote_name.clone(), err_msg))
                                        }
                                    }
                                } else {
                                    let err_msg = format!("Retry 'about' command failed for remote '{}' (Status {}).", remote_name, output.status);
                                    eprintln!("    Warning: {}", err_msg);
                                    Err((remote_name.clone(), err_msg))
                                }
                            }
                            Err(e) => {
                                let err_msg = format!("Failed to execute retry 'about' command for remote '{}': {}", remote_name, e);
                                eprintln!("    Warning: {}", err_msg);
                                Err((remote_name.clone(), err_msg))
                            }
                        };
                        final_about_results_for_report.push(retry_about_result);
                        // If user chose to reconnect, we keep it for lsjson processing by default.
                        
                    } else { // User chose NOT to reconnect
                        println!("  Skipping further processing (lsjson) for remote '{}' as per user choice.", remote_name);
                        remotes_for_lsjson_processing.retain(|r_name| r_name != &remote_name);
                        // Add the original error to the report
                        final_about_results_for_report.push(Err((remote_name, original_err_msg)));
                    }
                }
            }
        }
        
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        match cloudmapper::process_about_results(
            final_about_results_for_report, // Use the updated list
            about_output_path.to_str().unwrap_or(ABOUT_OUTPUT_FILE_NAME),
        ) {
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error processing 'about' results or writing report: {}", e);
                about_report_error_flag = true;
            }
        }
    } else {
        println!("'About' report generation skipped by flag.");
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        if about_output_path.exists() { let _ = fs::remove_file(&about_output_path); }
    }
    println!("{SECTION_SEPARATOR}");

    // --- 3. Process Remotes (lsjson) in Parallel using filtered list ---
    let mut lsjson_process_errors: u32 = 0;
    let mut remotes_processed_successfully_lsjson: u32 = 0;

    if remotes_for_lsjson_processing.is_empty() {
        println!("No remotes remaining for 'lsjson' processing after 'about' phase. Skipping 'lsjson'.");
    } else {
        println!("Processing {} remotes in parallel (rclone lsjson)...", remotes_for_lsjson_processing.len());
        let lsjson_start_time = Instant::now();

        let lsjson_results: Vec<RemoteProcessingResult> = remotes_for_lsjson_processing
            .par_iter()
            .map(|remote_name_str| {
                let remote_name = remote_name_str.clone();
                let start_lsjson_remote = Instant::now();
                let mut lsjson_args_owned: Vec<String> = common_rclone_args.clone();
                let remote_target = format!("{}:", remote_name);
                lsjson_args_owned.extend(vec![
                    "lsjson".to_string(),
                    "-R".to_string(),
                    "--hash".to_string(),
                    "--fast-list".to_string(),
                    remote_target,
                ]);
                let lsjson_args_for_cmd: Vec<&str> =
                    lsjson_args_owned.iter().map(|s| s.as_str()).collect();

                match cloudmapper::run_command(rclone_executable, &lsjson_args_for_cmd) {
                    Ok(output) => {
                        if output.status.success() {
                            let json_string = String::from_utf8_lossy(&output.stdout);
                            match cloudmapper::parse_rclone_lsjson(&json_string) {
                                Ok(raw_files) => {
                                    let duration = start_lsjson_remote.elapsed();
                                    println!(
                                        "  Finished lsjson for {} ({} items) in {:.2}s",
                                        remote_name,
                                        raw_files.len(),
                                        duration.as_secs_f32()
                                    );
                                    Ok((remote_name, raw_files))
                                }
                                Err(e) => {
                                    let err_msg = format!("Error parsing lsjson JSON for remote '{}': {}", remote_name, e);
                                    eprintln!("  {}", err_msg);
                                    Err((remote_name, err_msg))
                                }
                            }
                        } else {
                            let err_msg = format!("rclone lsjson command failed for remote '{}'. Status: {}.", remote_name, output.status);
                            eprintln!("  {}", err_msg);
                            Err((remote_name, err_msg))
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("Error executing rclone lsjson for remote '{}': {}", remote_name, e);
                        eprintln!("  {}", err_msg);
                        Err((remote_name, err_msg))
                    }
                }
            })
            .collect();

        let lsjson_duration = lsjson_start_time.elapsed();
        println!(
            "Finished parallel lsjson processing phase in {:.2}s.",
            lsjson_duration.as_secs_f32()
        );
        println!("{SECTION_SEPARATOR}");

        // --- 4. Aggregate lsjson Results Sequentially ---
        println!("Aggregating lsjson results...");
        for result in lsjson_results {
            match result {
                Ok((remote_name, raw_files)) => {
                    files_collection.add_remote_files(&remote_name, raw_files);
                    remotes_processed_successfully_lsjson += 1;
                }
                Err(_) => {
                    lsjson_process_errors += 1;
                }
            }
        }
        println!(
            "Lsjson aggregation complete. {} remotes processed successfully, {} errors encountered.",
            remotes_processed_successfully_lsjson, lsjson_process_errors
        );
    }
    println!("{SECTION_SEPARATOR}");
    

    // --- 5. Generate Standard Reports (if data exists) ---
    let mut report_generation_error_flag = false;

    if files_collection.files.is_empty() {
        if lsjson_process_errors > 0 || (!remotes_for_lsjson_processing.is_empty() && remotes_processed_successfully_lsjson == 0) {
             println!("Warning: No file data collected from 'lsjson'. Skipping standard reports (Tree, Size, Duplicates, Extensions, Largest Files, Treemap).");
        } else if remotes_for_lsjson_processing.is_empty() { // This implies no remotes were left after 'about' phase
             println!("Warning: No remotes were processed by 'lsjson' (all skipped or failed initial 'about'). Skipping standard reports.");
        }
        else { // This implies lsjson ran but found 0 files for all processed remotes
             println!("No files found across successfully processed remotes via 'lsjson'. Skipping standard reports.");
        }
    } else {
        println!("Generating standard reports...");
        let report_start_time = Instant::now();
        match cloudmapper::generate_reports(
            &mut files_collection,
            args.output_mode,
            args.duplicates,
            args.extensions_report,
            args.largest_files,
            args.html_treemap,
            &output_dir,
            TREE_OUTPUT_FILE_NAME,
            TREE_OUTPUT_FILE_NAME,
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
                println!(
                    "Standard reports generated successfully in {:.2}s.",
                    report_start_time.elapsed().as_secs_f32()
                );
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
                eprintln!("Error generating standard reports: {}", e);
                report_generation_error_flag = true;
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
    println!( "  Remotes attempted for 'lsjson': {}", remotes_for_lsjson_processing.len());
    println!(
        "  Remotes successfully processed via 'lsjson': {}",
        remotes_processed_successfully_lsjson
    );
    let total_errors = lsjson_process_errors
        + if report_generation_error_flag { 1 } else { 0 }
        + if about_report_error_flag { 1 } else { 0 };
    println!(
        "  Errors encountered during processing/reporting: {}",
        total_errors
    );
    println!("{SECTION_SEPARATOR}");

    // --- 7. Final Result ---
    if total_errors > 0 {
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