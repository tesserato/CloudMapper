use human_bytes::human_bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet}; // Combined imports
use std::fs;
use std::path::Path; // Added io for potential errors // Added for writing files

// --- Data Structures ---
// (Mostly unchanged, ensure necessary ones are public if accessed from main.rs)

// Make Hashes public if needed externally, otherwise keep private if only used within Files processing.
// Keep derive macros. Mark fields pub if direct access is needed, but methods are better.
#[derive(Serialize, Deserialize, Eq, Debug, Hash, Clone, Ord, PartialOrd, PartialEq)]
pub struct Hashes {
    #[serde(rename = "SHA-1", alias = "sha1")] // Handle potential case variations from rclone
    sha1: Option<String>,
    #[serde(rename = "DropboxHash", alias = "dropbox")]
    dropbox: Option<String>,
    #[serde(rename = "MD5", alias = "md5")]
    md5: Option<String>,
    #[serde(rename = "SHA-256", alias = "sha256")]
    sha256: Option<String>,
    #[serde(rename = "QuickXorHash", alias = "quickxor")]
    quickxor: Option<String>,
    // Add other hash types if rclone might output them (e.g., MailruHash, HidriveHash)
    // #[serde(rename = "MailruHash")] mailru: Option<String>,
}

// Keep this internal implementation detail
// (PartialEq implementation for Hashes seems overly complex,
// relying on Ord/PartialOrd derived from fields might be simpler if that's the intent.
// The current impl checks if *any* non-None hash matches)
// No change needed here based on requirements, but review if logic is correct.

// Make RawFile public as it's the direct result of parsing JSON
#[derive(Serialize, Deserialize, Eq, Debug, Clone)] // Added Clone
pub struct RawFile {
    #[serde(rename = "Path")]
    pub path: String, // Made pub
    #[serde(rename = "Name")]
    pub name: String, // Made pub
    #[serde(rename = "Size")]
    pub size: i64, // Made pub
    #[serde(rename = "MimeType")]
    pub mime_type: String, // Made pub
    #[serde(rename = "ModTime")]
    pub mod_time: String, // Made pub
    #[serde(rename = "IsDir")]
    pub is_dir: bool, // Made pub
    #[serde(rename = "Hashes")]
    pub hashes: Option<Hashes>, // Made pub
                                // ID: String, // Keep commented if not used
}

// Keep internal implementations for sorting RawFile
impl PartialEq for RawFile {
    fn eq(&self, other: &Self) -> bool {
        // Base equality check on fields relevant for comparison
        self.path == other.path
            && self.mod_time == other.mod_time
            && self.size == other.size
            && self.is_dir == other.is_dir
            && self.mime_type == other.mime_type
            && self.name == other.name
    }
}

impl PartialOrd for RawFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Keep original sorting logic (Dirs first, then by path depth, then path string)
        self.is_dir
            .cmp(&other.is_dir)
            .reverse() // true (dir) comes before false (file)
            .then_with(|| {
                self.path
                    .split('/')
                    .count()
                    .cmp(&other.path.split('/').count())
            })
            .then_with(|| self.path.cmp(&other.path))
    }
}

// Keep internal helper
fn get_name_and_extension(path: &str) -> (String, Option<String>) {
    let p = Path::new(path);
    let name = p
        .file_stem()
        .map_or_else(|| path.to_string(), |s| s.to_string_lossy().into_owned());
    let ext = p.extension().map(|s| s.to_string_lossy().into_owned());
    (name, ext)
}

// Internal representation, keep private unless needed externally
#[derive(Debug, Clone)]
pub struct File {
    service: String, // The original remote name
    ext: String,
    path: Vec<String>, // Path components without the service name
    modified: String,
    size: i64,                      // Keep as i64 to handle potential -1 from rclone
    is_dir: bool,                   // Store if it's a directory explicitly
    children_keys: HashSet<String>, // Keys of direct children
    hashes: Option<Hashes>,
}

impl File {
    // Updated factory method from RawFile
    fn from_raw(service: String, raw_file: &RawFile) -> Self {
        let path_components: Vec<String> = raw_file
            .path
            .split('/')
            .filter(|s| !s.is_empty()) // Handle potential leading/trailing slashes
            .map(String::from)
            .collect();

        let (name_part, ext_opt) = if raw_file.is_dir {
            // For directories, the last component is the name, extension is empty
            (path_components.last().cloned().unwrap_or_default(), None)
        } else {
            // For files, extract from Name field
            get_name_and_extension(&raw_file.name)
        };

        // Reconstruct path vec to potentially correct the last element if ext was stripped
        let mut final_path = path_components;
        if !raw_file.is_dir && ext_opt.is_some() {
            if let Some(last) = final_path.last_mut() {
                // Only update if the calculated name differs (safer)
                if *last != name_part {
                    *last = name_part;
                }
            }
        }

        Self {
            service,
            ext: ext_opt.unwrap_or_default(),
            path: final_path,
            modified: raw_file.mod_time.clone(),
            size: raw_file.size, // Keep original size, sum later during parsing
            is_dir: raw_file.is_dir,
            children_keys: HashSet::new(),
            hashes: raw_file.hashes.clone(), // Clone hashes
        }
    }

    // Generate a unique key for this file within the HashMap
    fn get_key(&self) -> String {
        // Combine service and full path for uniqueness
        // Using a separator unlikely to be in paths/service names
        if self.is_dir {
            return format!("{}{}", self.service, self.path.join(""));
        } else {
            format!(
                "{}{}{}{}",
                self.service,
                self.path.join(""),
                self.ext,
                self.size
            )
        }
    }

    // Generate the key for the potential parent directory
    fn get_parent_key(&self) -> Option<String> {
        if self.path.len() > 1 {
            let parent_path = &self.path[..self.path.len() - 1];
            Some(format!("{}{}", self.service, parent_path.join("")))
        } else {
            None // Root level file/dir in the service
        }
    }

    // Recursive function to format the output string and calculate total size
    // Takes the full map of files for lookups
    fn format_tree_entry(
        &self,
        indent_size: usize,
        all_files: &HashMap<String, File>,
    ) -> (String, u64) {
        let name = self.path.last().cloned().unwrap_or_else(|| "/".to_string()); // Handle root case?
        let indent = " ".repeat(indent_size * (self.path.len().saturating_sub(1)));
        let starter = if self.is_dir { "📁" } else { "" }; // Folder icon for dirs
        let dot_ext = if !self.is_dir && !self.ext.is_empty() {
            format!(".{}", self.ext)
        } else {
            "".to_string()
        };

        let mut total_size: u64 = if self.size >= 0 { self.size as u64 } else { 0 };
        let mut children_output = Vec::new();

        // Sort children for consistent output
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by_key(|key| {
            all_files
                .get(*key)
                .map_or("", |f| f.path.last().map_or("", |s| s.as_str()))
        });

        for key in sorted_children_keys {
            if let Some(child_file) = all_files.get(key) {
                let (child_str, child_size) = child_file.format_tree_entry(indent_size, all_files);
                children_output.push(child_str);
                total_size += child_size;
            } else {
                eprintln!("Warning: Child key '{}' not found in file map.", key);
            }
        }

        let size_str = human_bytes(total_size as f64);
        let modified_str = &self.modified; // Simplified display for now

        let children_str = children_output.join("\n");
        let line_sep = if children_output.is_empty() { "" } else { "\n" };

        let entry_str = format!(
            "{}{} {}{} 💾 {} 📅 {}",
            indent, starter, name, dot_ext, size_str, modified_str
        );

        (
            format!("{}{}{}", entry_str, line_sep, children_str),
            total_size,
        )
    }
}

// Internal struct for tracking duplicates
#[derive(Debug)]
struct DuplicateInfo {
    paths: Vec<String>, // List of full paths (service::path/to/file)
    size: u64,
}

// Make Files struct public - this is the main container
#[derive(Debug)]
pub struct Files {
    // Store File objects keyed by their unique path (service::path/to/file)
    pub files: HashMap<String, File>,
    // Store duplicates found, keyed by hash
    duplicates: BTreeMap<Hashes, DuplicateInfo>,
    // Keep track of root keys for each service for tree building
    roots_by_service: HashMap<String, Vec<String>>,
    // Store hashes encountered for faster duplicate lookup during addition
    // Maps Hash -> List of file keys having that hash
    hash_map: HashMap<Hashes, Vec<String>>,
}

impl Files {
    // Public constructor
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            duplicates: BTreeMap::new(),
            roots_by_service: HashMap::new(),
            hash_map: HashMap::new(),
        }
    }

    /// Adds files from a single `rclone lsjson` output (one remote)
    /// Parses RawFile into File and stores them.
    pub fn add_remote_files(&mut self, service_name: &str, raw_files: Vec<RawFile>) {
        println!("Adding files for service: {}", service_name);
        let mut service_root_keys = Vec::new();

        // Sort raw_files first - important for parent dirs likely appearing before children
        let mut sorted_raw_files = raw_files;
        sorted_raw_files.sort(); // Use the Ord impl for RawFile

        for raw_file in sorted_raw_files {
            // Skip empty paths or names if they occur
            if raw_file.path.is_empty() || raw_file.name.is_empty() {
                eprintln!(
                    "Warning: Skipping raw file with empty path/name: {:?}",
                    raw_file
                );
                continue;
            }

            let file = File::from_raw(service_name.to_string(), &raw_file);
            let key = file.get_key();

            // Store root keys
            if file.get_parent_key().is_none() {
                service_root_keys.push(key.clone());
            }

            // Add to hash map for duplicate checking
            if let Some(hashes) = &file.hashes {
                // Only consider files with hashes and non-negative size for duplicates
                if file.size >= 0 {
                    self.hash_map
                        .entry(hashes.clone())
                        .or_default()
                        .push(key.clone());
                }
            }

            // Add the file itself
            if self.files.insert(key.clone(), file).is_some() {
                // This shouldn't happen if keys are generated correctly (service::path)
                eprintln!("Warning: Overwriting existing file key: {}", key);
            }
        }
        self.roots_by_service
            .insert(service_name.to_string(), service_root_keys);
        println!(
            "Finished adding {} files for service: {}",
            self.files.len(),
            service_name
        );
    }

    /// Builds the tree structure by linking parents and children
    fn build_tree(&mut self) {
        let mut parent_child_map: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: Collect all parent-child relationships
        for (key, file) in &self.files {
            if let Some(parent_key) = file.get_parent_key() {
                // Check if parent exists before adding child
                if self.files.contains_key(&parent_key) {
                    parent_child_map
                        .entry(parent_key)
                        .or_default()
                        .insert(key.clone());
                } else {
                    // This might happen if a parent directory wasn't listed by rclone lsjson for some reason
                    // It might indicate an incomplete listing.
                    // We can't reliably build the tree in this case for this file.
                    eprintln!(
                        "Warning: Parent key '{}' not found for file '{}'. File might be orphaned in tree view.",
                        parent_key, key
                    );
                }
            }
            // Root files don't have parents, handled by roots_by_service map
        }

        // Second pass: Update the children_keys in each File struct
        for (parent_key, children_keys) in parent_child_map {
            if let Some(parent_file) = self.files.get_mut(&parent_key) {
                // Ensure the parent is marked as a directory (sometimes rclone might not?)
                // parent_file.is_dir = true;
                parent_file.children_keys = children_keys;
            }
            // If parent_key isn't in self.files, we already warned above.
        }
        println!("Tree structure built.");
    }

    /// Finds duplicate files based on stored hashes.
    fn find_duplicates(&mut self) {
        println!("Finding duplicates based on hashes...");
        self.duplicates.clear(); // Clear previous results if any

        for (hash, keys) in &self.hash_map {
            // We need at least two files with the same hash to have duplicates
            if keys.len() > 1 {
                let mut paths = Vec::new();
                let mut size: Option<u64> = None;

                for key in keys {
                    if let Some(file) = self.files.get(key) {
                        // Use the full key (service::path) as the identifier
                        paths.push(key.clone());
                        // Store size (assume all files with same hash have same size)
                        // Take the first valid size found.
                        if size.is_none() && file.size >= 0 {
                            size = Some(file.size as u64);
                        } else if size.is_some() && file.size >= 0 && size != Some(file.size as u64)
                        {
                            // This would be unusual - same hash, different size reported by rclone?
                            eprintln!(
                                "Warning: Files with same hash {:?} have different sizes: {} vs {}",
                                hash,
                                size.unwrap(),
                                file.size
                            ); // FIXME
                        }
                    }
                }

                // Only add if we found paths and a valid size
                if !paths.is_empty() && size.is_some() {
                    self.duplicates.insert(
                        hash.clone(),
                        DuplicateInfo {
                            paths,
                            size: size.unwrap_or(0),
                        },
                    );
                }
            }
        }
        println!("Found {} sets of duplicate files.", self.duplicates.len());
    }

    /// Generates the formatted tree string and total size
    fn generate_tree_output(&self) -> (String, u64) {
        let mut final_text: Vec<String> = Vec::new();
        let mut total_size: u64 = 0;

        // Sort services alphabetically for consistent output
        let mut sorted_services: Vec<&String> = self.roots_by_service.keys().collect();
        sorted_services.sort();

        for service_name in sorted_services {
            let mut service_total_size: u64 = 0;
            let mut service_entries: Vec<String> = Vec::new();

            // Get root keys for this service, handle if service has no roots (empty remote?)
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            // Sort root keys alphabetically for consistent output
            let mut sorted_root_keys = root_keys;
            sorted_root_keys.sort_by_key(|key| {
                self.files
                    .get(key)
                    .map_or("", |f| f.path.last().map_or("", |s| s.as_str()))
            });

            for root_key in &sorted_root_keys {
                if let Some(root_file) = self.files.get(root_key) {
                    // Start recursion from root files
                    let (entry_str, entry_size) = root_file.format_tree_entry(2, &self.files); // Use indent size 2
                    service_entries.push(entry_str);
                    service_total_size += entry_size;
                } else {
                    eprintln!(
                        "Warning: Root key '{}' not found in file map for service '{}'.",
                        root_key, service_name
                    );
                }
            }

            // Add service header
            let service_prefix = "➡️"; // Use a different prefix/icon for services
            final_text.push(format!(
                "{} {}: {}",
                service_prefix,
                service_name, // Use the actual service name
                human_bytes(service_total_size as f64)
            ));
            final_text.extend(service_entries); // Add entries for this service
            final_text.push("".to_string()); // Add a blank line between services

            total_size += service_total_size;
        }

        let header = format!(
            "Total size across all services: {}",
            human_bytes(total_size as f64)
        );
        final_text.insert(0, header.clone());
        final_text.insert(1, "=".repeat(header.len())); // Separator line
        final_text.push("=".repeat(header.len())); // Footer separator

        (final_text.join("\n"), total_size)
    }

    /// Generates the formatted duplicates string
    fn generate_duplicates_output(&self) -> String {
        if self.duplicates.is_empty() {
            return "No duplicate files found based on available hashes.".to_string();
        }

        // Sort duplicates for consistent output - e.g., by size descending * number of files
        let mut sorted_duplicates: Vec<(&Hashes, &DuplicateInfo)> =
            self.duplicates.iter().collect();
        sorted_duplicates.sort_by(|a, b| {
            let size_a = a.1.size * a.1.paths.len() as u64;
            let size_b = b.1.size * b.1.paths.len() as u64;
            size_b.cmp(&size_a) // Descending order
        });

        let mut lines: Vec<String> = Vec::new();
        lines.push("--- Duplicate Files Report ---".to_string());

        for (hashes, info) in sorted_duplicates {
            lines.push(format!(
                "\nDuplicates found with size: {} ({} files)",
                human_bytes(info.size as f64),
                info.paths.len()
            ));

            // List paths associated with this hash
            for path_key in &info.paths {
                // Path key is already service::path/to/file
                lines.push(format!("  - {}", path_key));
            }

            // List the hashes that matched
            lines.push("  Matching Hashes:".to_string());
            if let Some(h) = &hashes.md5 {
                lines.push(format!("    MD5: {}", h));
            }
            if let Some(h) = &hashes.sha1 {
                lines.push(format!("    SHA-1: {}", h));
            }
            if let Some(h) = &hashes.sha256 {
                lines.push(format!("    SHA-256: {}", h));
            }
            if let Some(h) = &hashes.dropbox {
                lines.push(format!("    DropboxHash: {}", h));
            }
            if let Some(h) = &hashes.quickxor {
                lines.push(format!("    QuickXorHash: {}", h));
            }
            // Add others if needed
        }
        lines.push("\n--- End of Report ---".to_string());
        lines.join("\n")
    }
}

// --- Public Functions ---

/// Parses the JSON output from `rclone lsjson`.
/// Returns a Vec of RawFile objects or a JSON parsing error.
pub fn parse_rclone_lsjson(json_data: &str) -> Result<Vec<RawFile>, serde_json::Error> {
    serde_json::from_str(json_data)
}

/// Processes the aggregated file data, builds the tree, finds duplicates (if requested),
/// and writes the output reports to disk.
pub fn generate_reports(
    files_data: &mut Files, // Needs to be mutable to build tree/find duplicates
    enable_duplicates_report: bool,
    tree_output_path: &str,       // e.g., "files.txt"
    duplicates_output_path: &str, // e.g., "duplicates.txt"
    size_output_path: &str,       // e.g., "size_used.txt"
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating final reports...");

    // 1. Build the internal tree structure
    files_data.build_tree();

    // 2. Generate Tree Report String and get total size
    let (tree_report_string, total_size) = files_data.generate_tree_output();
    let size_report_string = format!(
        "Total size across all services: {}",
        human_bytes(total_size as f64)
    );

    // 3. Write Tree Report and Size Report
    fs::write(tree_output_path, &tree_report_string)?;
    println!("Tree report written to '{}'", tree_output_path);
    fs::write(size_output_path, &size_report_string)?;
    println!("Size report written to '{}'", size_output_path);

    // 4. Generate and Write Duplicates Report (if enabled)
    if enable_duplicates_report {
        files_data.find_duplicates(); // Find duplicates based on collected hashes
        let duplicates_report_string = files_data.generate_duplicates_output();
        fs::write(duplicates_output_path, duplicates_report_string)?;
        println!("Duplicates report written to '{}'", duplicates_output_path);
    } else {
        println!("Duplicate detection skipped.");
        // Optionally delete the old duplicates file if it exists
        let _ = fs::remove_file(duplicates_output_path); // Ignore error if file doesn't exist
    }

    println!("Reports generated successfully.");
    Ok(())
}
