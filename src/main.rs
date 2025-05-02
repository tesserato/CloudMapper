// main.rs

// Declare the library module (expects lib.rs in the same src directory or configured in Cargo.toml)
mod lib;

use std::process::{Command, Stdio, Output};
use std::time::Instant;
use std::io::{self, Write, ErrorKind}; // Added ErrorKind
use std::fs; // Added for file system operations if needed outside lib

// --- Configuration ---
const RCLONE_EXECUTABLE: &str = "rclone"; // Or specify full path if not in PATH
// Output report filenames
const TREE_OUTPUT_FILE: &str = "files.txt";
const DUPLICATES_OUTPUT_FILE: &str = "duplicates.txt";
const SIZE_OUTPUT_FILE: &str = "size_used.txt";
// Set to true to enable duplicate detection and generate duplicates.txt
const ENABLE_DUPLICATES_REPORT: bool = true;
// --- End Configuration ---


// Helper to run a command and capture output, providing better error context
fn run_command(cmd_name: &str, args: &[&str]) -> Result<Output, io::Error> {
    println!("> Running: {} {}", cmd_name, args.join(" "));
    let output = Command::new(cmd_name)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output(); // Execute and wait

    match output {
        Ok(ref out) => {
            if !out.status.success() {
                 // stderr is often useful when the command fails
                 eprintln!("  Command failed with status: {}", out.status);
                 let stderr_str = String::from_utf8_lossy(&out.stderr);
                 if !stderr_str.is_empty() {
                     eprintln!("  stderr:\n---\n{}\n---", stderr_str.trim());
                 }
            }
        }
        Err(ref e) => {
            eprintln!("  Failed to execute command '{}': {}", cmd_name, e);
            if e.kind() == ErrorKind::NotFound {
                 eprintln!("  Hint: Make sure '{}' is installed and accessible in your system's PATH.", cmd_name);
            }
        }
    }
    output // Return the original result
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting rclone data processing...");
    let overall_start_time = Instant::now();

    // --- 1. Initialize Files container from lib ---
    let mut files_collection = lib::Files::new();

    // --- 2. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");
    let remote_names: Vec<String> = match run_command(RCLONE_EXECUTABLE, &["listremotes"]) {
        Ok(output) => {
            if !output.status.success() {
                // Error already printed by run_command helper
                return Err(Box::new(io::Error::new(
                    ErrorKind::Other,
                    format!("'{} listremotes' failed.", RCLONE_EXECUTABLE),
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
            // Error already printed by run_command helper
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
        let lsjson_args = &["lsjson", "-R", "--hash", "--fast-list", remote_name];
        match run_command(RCLONE_EXECUTABLE, lsjson_args) {
            Ok(output) => {
                if output.status.success() {
                    let json_string = String::from_utf8_lossy(&output.stdout);

                    // --- 3b. Parse JSON using lib function ---
                    match lib::parse_rclone_lsjson(&json_string) {
                        Ok(raw_files) => {
                             let file_count = raw_files.len();
                            // --- 3c. Add parsed data using lib function ---
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
                            // Optionally save the problematic JSON for debugging
                            // let _ = fs::write(format!("{}_error.json", remote_name.replace(":", "_")), &output.stdout);
                            process_errors += 1;
                        }
                    }
                } else {
                    // rclone lsjson command failed, error already printed by run_command
                    process_errors += 1;
                     eprintln!("  Failed to list items for remote '{}'.", remote_name);
                }
            }
            Err(_) => {
                // Failed to execute rclone lsjson, error already printed by run_command
                process_errors += 1;
                 eprintln!("  Skipping remote '{}' due to execution failure.", remote_name);
            }
        }
        println!("------------------------------------------");

    } // End of remote loop

    let loop_duration = loop_start_time.elapsed();
    println!("Finished processing all remotes in {:.2}s.", loop_duration.as_secs_f32());
    println!("==========================================");


    // --- 4. Generate Reports (only if some data was collected) ---
    if files_collection.files.is_empty() {
         if process_errors > 0 {
             println!("No file data was collected due to processing errors. Skipping report generation.");
         } else {
             println!("No files found across any successfully processed remotes. Skipping report generation.");
         }
    } else {
        println!("Generating final reports...");
        let report_start_time = Instant::now();
        match lib::generate_reports(
            &mut files_collection, // Pass mutable reference
            ENABLE_DUPLICATES_REPORT,
            TREE_OUTPUT_FILE,
            DUPLICATES_OUTPUT_FILE,
            SIZE_OUTPUT_FILE,
        ) {
            Ok(_) => {
                 println!(
                     "Reports generated successfully in {:.2}s.",
                     report_start_time.elapsed().as_secs_f32()
                 );
            },
            Err(e) => {
                eprintln!("Error generating final reports: {}", e);
                // Treat report generation failure as a process error
                process_errors += 1;
            }
        }
    }


    // --- 5. Print Summary ---
    let total_duration = overall_start_time.elapsed();
    println!("==========================================");
    println!("Processing Summary:");
    println!("  Total time: {:.2}s", total_duration.as_secs_f32());
    println!("  Remotes found: {}", remote_names.len());
    println!("  Remotes processed successfully: {}", remotes_processed_successfully);
    println!("  Processing errors encountered: {}", process_errors);
    println!("==========================================");

    // Optionally return an error from main if any errors occurred
    if process_errors > 0 {
        eprintln!("Completed with errors.");
        // Return a generic error to indicate failure
        // std::process::exit(1); // Or exit directly
         return Err(Box::new(io::Error::new(ErrorKind::Other, "Processing completed with errors")));
    }

    println!("Processing completed successfully.");
    Ok(())
}