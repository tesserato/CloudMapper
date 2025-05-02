use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread; // Used for potential retries or parallelism later if needed
use std::time::Duration;
use std::time::Instant; // Used for potential sleep on retry

// --- Configuration ---
const RCLONE_EXECUTABLE: &str = "rclone"; // Or specify full path if not in PATH
const OUTPUT_ROOT_DIR: &str = "./rclone_json_output"; // Directory to save JSON files
                                                      // --- End Configuration ---

// Function to sanitize remote names for use in filenames
fn sanitize_filename(name: &str) -> String {
    name.replace(':', "")
        .replace('.', "_DOT_")
        .replace('@', "_AT_")
        .replace(' ', "_")
        .trim() // Remove leading/trailing whitespace just in case
        .to_string()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting rclone lsjson process...");

    // --- 1. Create Output Directory ---
    // Ensure the output directory exists, create it if not.
    std::fs::create_dir_all(OUTPUT_ROOT_DIR)?;
    println!("Output directory '{}' ensured.", OUTPUT_ROOT_DIR);

    // --- 2. Get List of Remotes ---
    println!("Fetching list of rclone remotes...");
    let list_remotes_output = Command::new(RCLONE_EXECUTABLE).arg("listremotes").output(); // Capture output

    let remote_names: Vec<String> = match list_remotes_output {
        Ok(output) => {
            if !output.status.success() {
                eprintln!(
                    "Error running '{} listremotes': {}",
                    RCLONE_EXECUTABLE, output.status
                );
                eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
                // Using a more specific error type or Box<dyn Error>
                return Err(Box::new(io::Error::new(
                    ErrorKind::Other,
                    "Failed to list rclone remotes",
                )));
            }
            let stdout_str = String::from_utf8_lossy(&output.stdout);
            // Split into lines, trim whitespace, filter out empty lines, collect
            stdout_str
                .lines()
                .map(|line| line.trim())
                .filter(|line| !line.is_empty())
                .map(String::from) // Convert &str to String
                .collect()
        }
        Err(e) => {
            eprintln!(
                "Failed to execute '{} listremotes': {}",
                RCLONE_EXECUTABLE, e
            );
            eprintln!(
                "Ensure '{}' is installed and in your PATH.",
                RCLONE_EXECUTABLE
            );
            return Err(Box::new(e)); // Propagate the error
        }
    };

    if remote_names.is_empty() {
        println!("No rclone remotes found. Exiting.");
        return Ok(());
    }

    println!("Found remotes: {:?}", remote_names);

    // --- 3. Record Start Time ---
    let start_time = Instant::now();
    let mut success_count = 0;
    let mut error_count = 0;

    // --- 4. Iterate Through Remotes and Run lsjson ---
    for remote_name in &remote_names {
        println!("------------------------------------------");
        println!("Processing remote: {}", remote_name);

        // Sanitize name and create output path
        let sanitized_name = sanitize_filename(remote_name);
        let mut output_path = PathBuf::from(OUTPUT_ROOT_DIR);
        output_path.push(format!("{}.json", sanitized_name));

        println!("  Output file: {}", output_path.display());

        // Build the rclone lsjson command
        // Use Stdio::piped() for stdout to potentially stream large outputs later,
        // although here we capture it fully with output().
        let lsjson_cmd = Command::new(RCLONE_EXECUTABLE)
            .args(&[
                "lsjson",
                "-R",          // Recursive
                "--hash",      // Include hashes (MD5, SHA1, etc. depending on remote)
                "--fast-list", // Use faster listing method if available
                remote_name,   // The remote name itself
            ])
            .stdout(Stdio::piped()) // We want to capture stdout
            .stderr(Stdio::piped()) // Capture stderr too for error reporting
            .output(); // Execute and wait for completion

        match lsjson_cmd {
            Ok(output) => {
                if output.status.success() {
                    // Write the captured stdout (JSON data) to the file
                    match File::create(&output_path) {
                        Ok(mut file) => {
                            if let Err(e) = file.write_all(&output.stdout) {
                                eprintln!(
                                    "  Error writing JSON to file {}: {}",
                                    output_path.display(),
                                    e
                                );
                                error_count += 1;
                            } else {
                                println!(
                                    "  Successfully wrote JSON for {} to {}",
                                    remote_name,
                                    output_path.display()
                                );
                                success_count += 1;
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "  Error creating output file {}: {}",
                                output_path.display(),
                                e
                            );
                            error_count += 1;
                        }
                    }
                } else {
                    // rclone lsjson command failed
                    eprintln!(
                        "  Error running rclone lsjson for {}: {}",
                        remote_name, output.status
                    );
                    // Print stderr from the failed rclone command
                    let stderr_str = String::from_utf8_lossy(&output.stderr);
                    if !stderr_str.is_empty() {
                        eprintln!("  rclone stderr:\n---\n{}\n---", stderr_str.trim());
                    }
                    error_count += 1;
                }
            }
            Err(e) => {
                // Failed even to start the rclone lsjson process
                eprintln!(
                    "  Error executing rclone lsjson command for {}: {}",
                    remote_name, e
                );
                error_count += 1;
            }
        }
    }

    // --- 5. Calculate and Print Duration ---
    let end_time = Instant::now();
    let duration = end_time.duration_since(start_time);
    let total_seconds = duration.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;

    println!("==========================================");
    println!("Processing finished.");
    println!("Successfully processed: {}", success_count);
    println!("Failed attempts:        {}", error_count);
    println!(
        "Total duration: {:02} min {:02} sec ({:.3} s)",
        minutes,
        seconds,
        duration.as_secs_f64()
    );
    println!("==========================================");

    // Return Ok if the overall process structure completed,
    // even if individual remotes failed. Modify if strict success is needed.
    if error_count > 0 {
        // Optionally return an error if *any* remote failed
        // return Err(Box::new(io::Error::new(ErrorKind::Other, "One or more remotes failed")));
        println!("Completed with some errors.");
    } else {
        println!("All remotes processed successfully.");
    }

    Ok(())
}
