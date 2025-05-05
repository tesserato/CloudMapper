use clap::Parser;
use rayon::prelude::*; // Import Rayon traits for parallel iterators

use std::fs;
use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::time::Instant;

use cloudmapper::{AboutResult, OutputMode, RcloneAboutInfo};

// --- Configuration Constants (Base Filenames) ---
const TREE_OUTPUT_FILE_NAME: &str = "files.txt"; // Used in Single/Remote modes and as Folder content filename
const DUPLICATES_OUTPUT_FILE_NAME: &str = "duplicates.txt";
const SIZE_OUTPUT_FILE_NAME: &str = "size_used.txt";
const ABOUT_OUTPUT_FILE_NAME: &str = "about.txt";
const EXTENSIONS_OUTPUT_FILE_NAME: &str = "extensions.txt";
const TREEMAP_OUTPUT_FILE_NAME: &str = "treemap.html"; // New filename for HTML treemap

const REMOTE_ICON: &str = "☁️";
const FOLDER_ICON: &str = "📁";
const FILE_ICON: &str = "📄";
const SIZE_ICON: &str = "💽";
const DATE_ICON: &str = "📆";

const SECTION_SEPARATOR: &str =
    "==========================================================================================";

// --- Command Line Argument Definition ---
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to a rclone executable (optional). If not provided, Rclone from PATH variable will be used.
    #[arg(long, short = 'r', env = "RCLONE_EXECUTABLE")]
    rclone_path: Option<String>,

    /// Path to a rclone config file (optional). If not provided, global configuration will be used.
    #[arg(long, short = 'c', env = "RCLONE_CONFIG")]
    rclone_config: Option<String>,

    /// Path to the directory where the reports will be saved.
    #[arg(long, short = 'o', env = "CM_OUTPUT", default_value = "./cloud")]
    output_path: String,

    /// How to divide the outputs.
    #[arg(
        long,
        short = 'm',
        value_enum, // Use clap's value_enum feature
        default_value_t = OutputMode::Folders, // Default to folder structure
        env = "CM_OUTPUT_MODE"
    )]
    output_mode: OutputMode,

    /// Enable duplicate file detection report.
    #[arg(long, short = 'd', default_value_t = true, env = "CM_DUPLICATES")]
    duplicates: bool,

    /// Enable the file extensions report.
    #[arg(long, short = 'e', default_value_t = true, env = "CM_EXTENSIONS")]
    extensions_report: bool,

    /// Enable the 'rclone about' report for remote sizes.
    #[arg(long, short = 'a', default_value_t = true, env = "CM_ABOUT")]
    about_report: bool,

    /// Enable the HTML treemap visualization report.
    #[arg(long, short = 't', default_value_t = true, env = "CM_HTML_TREEMAP")]
    html_treemap: bool, // New flag

    /// Clean the output directory before generating reports.
    #[arg(long, short = 'k', default_value_t = true, env = "CM_CLEAN_OUTPUT")]
    clean_output: bool,
}

// Define a type alias for the result of processing a single remote with rclone lsjson command
type RemoteProcessingResult = Result<(String, Vec<cloudmapper::RawFile>), (String, String)>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Parse Command Line Arguments ---
    let args = Args::parse();

    // --- Determine rclone executable path ---
    // Use provided path, or default to "rclone"
    let rclone_executable = args.rclone_path.as_deref().unwrap_or("rclone");

    // Print config info
    println!("Using rclone executable: {}", rclone_executable);
    if let Some(conf) = &args.rclone_config {
        println!("Using rclone config: {}", conf);
    }
    println!("Output directory: {}", args.output_path);
    println!("Output mode: {:?}", args.output_mode);
    println!("Clean output directory: {}", args.clean_output);
    println!("Duplicates report enabled: {}", args.duplicates);
    println!("Extensions report enabled: {}", args.extensions_report);
    println!("About report enabled: {}", args.about_report);
    println!("HTML Treemap report enabled: {}", args.html_treemap); // Print new flag status
    println!("{SECTION_SEPARATOR}");

    println!("Starting rclone data processing...");
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

    let mut files_collection = cloudmapper::Files::new();

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
                    .filter(|line| !line.is_empty() && line.ends_with(':'))
                    .map(|line| line.trim_end_matches(':').to_string())
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
    println!("{SECTION_SEPARATOR}");

    // --- 3. Process Remotes (lsjson) in Parallel ---
    println!("Processing remotes in parallel...");
    let lsjson_start_time = Instant::now();
// Use rayon's par_iter to map over remote names in parallel
    let lsjson_results: Vec<RemoteProcessingResult> = remote_names
        .par_iter()
        .map(|remote_name| {
            // println!("  Starting lsjson for: {}", remote_name); // Reduced noise
            let start = Instant::now();
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
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        let err_msg = format!(
                            "rclone lsjson command failed for remote '{}'. Status: {}. Stderr: {}",
                            remote_name,
                            output.status,
                            stderr.trim()
                        ); // stderr already printed by run_command
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

    let lsjson_duration = lsjson_start_time.elapsed();
    println!(
        "Finished parallel lsjson phase in {:.2}s.",
        lsjson_duration.as_secs_f32()
    );
    println!("{SECTION_SEPARATOR}");
    println!("Aggregating lsjson results...");

    // --- 3b. Aggregate lsjson Results Sequentially ---
    let mut lsjson_process_errors: u32 = 0;
    let mut remotes_processed_successfully: u32 = 0;
    for result in lsjson_results {
        match result {
            Ok((remote_name, raw_files)) => {
                files_collection.add_remote_files(&remote_name, raw_files);
                remotes_processed_successfully += 1;
            }
            Err(_) => {
                lsjson_process_errors += 1;
            }
        }
    }

    // --- 4. Generate Standard Reports (if data exists) ---
    let mut report_generation_error = false;
    if files_collection.files.is_empty() {
        // Handle cases where no files were found or processed
        if lsjson_process_errors > 0 {
            println!(
                "No file data collected due to lsjson errors. Skipping standard report generation."
            );
        } else if remotes_processed_successfully == 0 && !remote_names.is_empty() {
            println!("No data successfully processed from any remote via lsjson. Skipping standard report generation.");
        } else {
            println!("No files found across successfully processed remotes. Skipping standard report generation.");
        }
        // Still allow 'about' report to run even if no files found, as long as remotes exist
        if !args.about_report || remote_names.is_empty() {
            println!("No reports to generate. Exiting.");
            return Ok(());
        } else {
            println!("Proceeding with 'about' report generation only.");
        }
    } else {
        // Generate Standard Reports (tree/files, size_used, duplicates, extensions, treemap)
        println!("Generating standard reports...");
        let report_start_time = Instant::now();

        match cloudmapper::generate_reports(
            &mut files_collection,
            args.output_mode,
            args.duplicates,
            args.extensions_report,
            args.html_treemap, // Pass the new flag
            &output_dir,
            TREE_OUTPUT_FILE_NAME,
            TREE_OUTPUT_FILE_NAME, // Use same base name for folder content filename
            DUPLICATES_OUTPUT_FILE_NAME,
            SIZE_OUTPUT_FILE_NAME,
            EXTENSIONS_OUTPUT_FILE_NAME,
            TREEMAP_OUTPUT_FILE_NAME, // Pass the new filename
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
                // Print paths to generated files
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
                if args.html_treemap { // Print path for treemap if generated
                    println!(
                        "  HTML Treemap report: {}",
                        output_dir.join(TREEMAP_OUTPUT_FILE_NAME).display()
                    );
                }
            }
            Err(e) => {
                eprintln!("Error generating standard reports: {}", e);
                report_generation_error = true;
            }
        }
    }
    println!("{SECTION_SEPARATOR}");

    // --- 5. Generate About Report (Parallel Fetch, Sequential Process) ---
    let mut about_report_error = false;
    if args.about_report {
        println!("Fetching 'about' info in parallel...");
        let about_fetch_start_time = Instant::now();
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);

        // Determine which services to query
        let services_to_query = if !files_collection.roots_by_service.is_empty() {
            files_collection.get_service_names()
        } else {
            remote_names.clone() // Fallback to initial list
        };

        if !services_to_query.is_empty() {
            // Parallel fetch and parse
            let about_results: Vec<AboutResult> = services_to_query
                .par_iter()
                .map(|remote_name| {
                    let remote_target = format!("{}:", remote_name);
                    let mut about_args_owned: Vec<String> = common_rclone_args.clone();
                    about_args_owned.extend(vec![
                        "about".to_string(),
                        remote_target,
                        "--json".to_string(),
                    ]);
                    let about_args_for_cmd: Vec<&str> =
                        about_args_owned.iter().map(|s| s.as_str()).collect();

                    match cloudmapper::run_command(rclone_executable, &about_args_for_cmd) {
                        Ok(output) => {
                            if output.status.success() {
                                let json_string = String::from_utf8_lossy(&output.stdout);
                                match serde_json::from_str::<RcloneAboutInfo>(&json_string) {
                                    Ok(info) => Ok((remote_name.clone(), info)),
                                    Err(e) => {
                                        let err_msg =
                                            format!("Failed to parse 'about' JSON: {}", e);
                                        Err((remote_name.clone(), err_msg))
                                    }
                                }
                            } else {
                                let stderr = String::from_utf8_lossy(&output.stderr);
                                let err_msg = format!(
                                    "Command failed (Status {}). Stderr: {}",
                                    output.status,
                                    stderr.trim()
                                );
                                Err((remote_name.clone(), err_msg))
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("Failed to execute 'about' command: {}", e);
                            Err((remote_name.clone(), err_msg))
                        }
                    }
                })
                .collect();

            let about_fetch_duration = about_fetch_start_time.elapsed();
            println!(
                "Finished parallel 'about' fetching in {:.2}s.",
                about_fetch_duration.as_secs_f32()
            );

            // Sequential processing and writing
            match cloudmapper::process_about_results(
                about_results,
                about_output_path.to_str().unwrap_or_else(|| {
                    eprintln!(
                        "Warning: Could not convert about output path to string. Using default."
                    );
                    ABOUT_OUTPUT_FILE_NAME // Use default name if path conversion fails
                }),
            ) {
                Ok(_) => { /* Success message printed by process_about_results */ }
                Err(e) => {
                    eprintln!("Error processing 'about' results or writing report: {}", e);
                    about_report_error = true;
                }
            }
        } else {
            println!("Skipping 'about' report as no remotes were identified for querying.");
            if about_output_path.exists() {
                let _ = fs::remove_file(&about_output_path);
            }
        }
    } else {
        println!("'About' report generation skipped by flag.");
        let about_output_path = output_dir.join(ABOUT_OUTPUT_FILE_NAME);
        if about_output_path.exists() {
            let _ = fs::remove_file(&about_output_path);
        }
    }

    // --- 6. Print Summary ---
    let total_duration = overall_start_time.elapsed();
    println!("{SECTION_SEPARATOR}");
    println!("Processing Summary:");
    println!("  Total time: {:.2}s", total_duration.as_secs_f32());
    println!("  Remotes found: {}", remote_names.len());
    println!(
        "  Remotes successfully processed (lsjson): {}",
        remotes_processed_successfully
    );
    let total_errors = lsjson_process_errors
        + if report_generation_error { 1 } else { 0 }
        + if about_report_error { 1 } else { 0 }; // Note: about_report_error currently not set by process_about_results on fetch errors
    println!(
        "  Processing/Reporting errors encountered: {}",
        total_errors
    ); // This mainly counts lsjson errors now
    println!("{SECTION_SEPARATOR}");

    if total_errors > 0 || report_generation_error || about_report_error {
        eprintln!("Completed with errors.");
        // Return generic error if any part failed
        return Err(Box::new(io::Error::new(
            ErrorKind::Other,
            "Processing completed with errors",
        )));
    }

    println!("Processing completed successfully.");
    Ok(())
}