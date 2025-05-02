mod lib;

// Import clap
use clap::Parser;

use std::fs;
use std::io::{self, ErrorKind, Write};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::Instant; // Import PathBuf for path manipulation

// --- Configuration Constants (Base Filenames) ---
// We keep these for the base names, the directory comes from args
const TREE_OUTPUT_FILE_NAME: &str = "files.txt";
const DUPLICATES_OUTPUT_FILE_NAME: &str = "duplicates.txt";
const SIZE_OUTPUT_FILE_NAME: &str = "size_used.txt";
const ENABLE_DUPLICATES_REPORT: bool = true; // Keep this toggle if needed, or make it an arg too
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
    // Example: Add the duplicate flag as an argument too
    // /// Enable duplicate file detection report
    // #[arg(long, short = 'd', default_value_t = true)] // Or false if default off
    // duplicates: bool,
}


/// Runs a command, capturing and displaying stdout and stderr.
///
/// If the command fails (exits with non-zero status), the stderr is displayed
/// and an error is returned. If the command is not found, a hint is printed to
/// verify the executable is installed or the path is correct.
///
/// The provided `args` are joined carefully to handle spaces in the arguments.
/// The display string will be constructed with double quotes around the
/// arguments if they contain spaces.
fn run_command(executable: &str, args: &[&str]) -> Result<Output, io::Error> {
    // Construct display string carefully, handling potential spaces in args
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
        .output(); // Execute and wait

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
    output // Return the original result
}

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
    let mut files_collection = lib::Files::new();

    // --- 2. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");

    // Build common args (potentially including --config)
    let mut common_rclone_args: Vec<&str> = Vec::new();
    if let Some(config_path) = &args.rclone_config {
        common_rclone_args.push("--config");
        common_rclone_args.push(config_path); // config_path lives long enough
    }

    let mut list_remotes_args = common_rclone_args.clone(); // Start with common args
    list_remotes_args.push("listremotes");

    let remote_names: Vec<String> = match run_command(rclone_executable, &list_remotes_args) {
        Ok(output) => {
            if !output.status.success() {
                return Err(Box::new(io::Error::new(
                    ErrorKind::Other,
                    format!("'{} listremotes' failed.", rclone_executable),
                )));
            }
            let stdout_str = String::from_utf8_lossy(&output.stdout);
            stdout_str
                .lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .map(String::from)
                .collect()
        }
        Err(e) => {
            return Err(Box::new(e)); // Propagate the execution error
        }
    };

    if remote_names.is_empty() {
        println!("No rclone remotes found or 'rclone listremotes' failed. Exiting.");
        return Ok(());
    }

    println!("Found {} remotes: {:?}", remote_names.len(), remote_names);
    println!("==========================================");

    // --- 3. Iterate Through Remotes and Process lsjson ---
    let mut process_errors: u32 = 0;
    let mut remotes_processed_successfully: u32 = 0;
    let loop_start_time = Instant::now();

    for remote_name in &remote_names {
        println!("Processing remote: {}", remote_name);
        let remote_start_time = Instant::now();

        // --- 3a. Run rclone lsjson ---
        // Build args for this specific command
        let mut lsjson_args = common_rclone_args.clone(); // Start with common args
        lsjson_args.extend(&["lsjson", "-R", "--hash", "--fast-list", remote_name]); // Add lsjson specific args

        match run_command(rclone_executable, &lsjson_args) {
            // Pass dynamic args
            Ok(output) => {
                if output.status.success() {
                    let json_string = String::from_utf8_lossy(&output.stdout);
                    match lib::parse_rclone_lsjson(&json_string) {
                        Ok(raw_files) => {
                            let file_count = raw_files.len();
                            files_collection.add_remote_files(remote_name, raw_files);
                            remotes_processed_successfully += 1;
                            println!(
                                "  Successfully processed {} items for '{}' in {:.2}s",
                                file_count,
                                remote_name,
                                remote_start_time.elapsed().as_secs_f32()
                            );
                        }
                        Err(e) => {
                            eprintln!("  Error parsing JSON for remote '{}': {}", remote_name, e);
                            process_errors += 1;
                        }
                    }
                } else {
                    process_errors += 1;
                    eprintln!("  Failed to list items for remote '{}'.", remote_name);
                }
            }
            Err(_) => {
                process_errors += 1;
                eprintln!(
                    "  Skipping remote '{}' due to execution failure.",
                    remote_name
                );
            }
        }
        println!("------------------------------------------");
    } // End of remote loop

    let loop_duration = loop_start_time.elapsed();
    println!(
        "Finished processing all remotes in {:.2}s.",
        loop_duration.as_secs_f32()
    );
    println!("==========================================");

    // --- 4. Generate Reports (only if some data was collected) ---
    if files_collection.files.is_empty() {
        // Message depends on whether errors occurred
        if process_errors > 0 {
            println!(
                "No file data was collected due to processing errors. Skipping report generation."
            );
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

        match lib::generate_reports(
            &mut files_collection,
            ENABLE_DUPLICATES_REPORT, // Could also use args.duplicates if added
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
                process_errors += 1;
            }
        }
    }

    // --- 5. Print Summary ---
    // (Summary printing code remains the same)
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
