use human_bytes::human_bytes;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering; // Added for custom sorting
use std::collections::{BTreeMap, HashMap, HashSet}; // Combined imports
use std::fs;
use std::io::{self, ErrorKind}; // Added for run_command errors
use std::path::Path; // Added for writing files, PathBuf for directory manipulation
use std::process::{Command, Output, Stdio}; // Added for run_command

// Import the enum defined in main.rs - requires main.rs to define it publicly or pass instances
// For simplicity, let's assume main.rs passes an instance of its enum.
// We'll refer to it conceptually as OutputDivisionMode here.
// Alternatively, define a matching enum here. Let's define it here to keep lib self-contained conceptually.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputDivisionMode {
    Single,
    Remote,
    Folder,
}

// --- Data Structures ---
#[derive(Serialize, Deserialize, Eq, Debug, Hash, Clone, Ord, PartialOrd, PartialEq)]
pub struct Hashes {
    #[serde(rename = "SHA-1", alias = "sha1")]
    sha1: Option<String>,
    #[serde(rename = "DropboxHash", alias = "dropbox")]
    dropbox: Option<String>,
    #[serde(rename = "MD5", alias = "md5")]
    md5: Option<String>,
    #[serde(rename = "SHA-256", alias = "sha256")]
    sha256: Option<String>,
    #[serde(rename = "QuickXorHash", alias = "quickxor")]
    quickxor: Option<String>,
}

#[derive(Serialize, Deserialize, Eq, Debug, Clone)]
pub struct RawFile {
    #[serde(rename = "Path")]
    pub path: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Size")]
    pub size: i64,
    #[serde(rename = "MimeType")]
    pub mime_type: String,
    #[serde(rename = "ModTime")]
    pub mod_time: String,
    #[serde(rename = "IsDir")]
    pub is_dir: bool,
    #[serde(rename = "Hashes")]
    pub hashes: Option<Hashes>,
}

impl PartialEq for RawFile {
    fn eq(&self, other: &Self) -> bool {
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
        self.is_dir
            .cmp(&other.is_dir)
            .reverse() // Dirs first when processing raw list
            .then_with(|| {
                self.path
                    .split('/')
                    .count()
                    .cmp(&other.path.split('/').count())
            })
            .then_with(|| self.path.cmp(&other.path))
    }
}

// Structure for deserializing `rclone about --json` output
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")] // Handles fields like "camelCase" if needed, though rclone uses lowercase
struct RcloneAboutInfo {
    total: Option<u64>,
    used: Option<u64>,
    trashed: Option<u64>,
    other: Option<u64>,
    free: Option<u64>,
}

fn get_name_and_extension(path: &str) -> (String, Option<String>) {
    let p = Path::new(path);
    let name = p
        .file_stem()
        .map_or_else(|| path.to_string(), |s| s.to_string_lossy().into_owned());
    let ext = p.extension().map(|s| s.to_string_lossy().into_owned());
    (name, ext)
}

#[derive(Debug, Clone)]
pub struct File {
    service: String, // The original remote name
    ext: String,
    path: Vec<String>, // Path components (e.g., ["folder1", "file.txt"])
    modified: String,
    size: i64,                      // Keep as i64 to handle potential -1 from rclone
    is_dir: bool,                   // Store if it's a directory explicitly
    children_keys: HashSet<String>, // Keys of direct children
    hashes: Option<Hashes>,
}

impl File {
    // from_raw, get_key, get_parent_key, get_display_name remain unchanged
    fn from_raw(service: String, raw_file: &RawFile) -> Self {
        let path_components: Vec<String> = raw_file
            .path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let (name_part, ext_opt) = if raw_file.is_dir {
            // For directories, the last component is the name, extension is empty
            (path_components.last().cloned().unwrap_or_default(), None)
        } else {
            // For files, extract from Name field
            get_name_and_extension(&raw_file.name)
        };
        let mut final_path = path_components;
        // Correction logic for final_path component based on name_part remains complex,
        // but assuming it works correctly for path representation.
        if !raw_file.is_dir && ext_opt.is_some() {
            if let Some(last) = final_path.last_mut() {
                // Basic check to avoid replacing a component like 'archive.tar.gz' with 'archive.tar'
                if !last.ends_with(&format!(".{}", ext_opt.as_deref().unwrap_or(""))) {
                    // Check if the name part derived from `Name` differs significantly from the last path component
                    // This aims to handle cases where `Name` is just the filename, but `Path` includes directories.
                    if *last != name_part {
                        // This part needs careful validation based on expected rclone output variations.
                        // For simplicity, we often trust `path_components` derived from `Path` field.
                        // If name_part is just the filename, we might not need to change `last`.
                        // Let's keep the previous logic but be aware it might need refinement.
                        if !last.ends_with(&format!(".{}", ext_opt.as_deref().unwrap_or(""))) {
                            *last = name_part; // Apply if last component doesn't look like it includes the ext
                        }
                    }
                }
            }
        }

        Self {
            service,
            ext: ext_opt.unwrap_or_default(),
            path: final_path, // Contains components like ["dir1", "subdir", "file"]
            modified: raw_file.mod_time.clone(),
            size: raw_file.size,
            is_dir: raw_file.is_dir,
            children_keys: HashSet::new(),
            hashes: raw_file.hashes.clone(),
        }
    }

    // Generate a unique key for this file within the HashMap
    fn get_key(&self) -> String {
        // Key needs to uniquely identify this node, considering service and path.
        // For directories, path is enough. For files, add name+ext+size to disambiguate
        // files with the same name but potentially different content/metadata at the same path level
        // (though less common with rclone lsjson output structure).
        if self.is_dir {
            return format!("{}{}", self.service, self.path.join("/"));
        } else {
            format!(
                "{}{}{}{}",
                self.service,
                self.path.join("/"),
                self.ext,
                self.size
            )
        }
    }

    // Generate the key for the potential parent directory
    fn get_parent_key(&self) -> Option<String> {
        if self.path.len() > 1 {
            let parent_path_vec = &self.path[..self.path.len() - 1];
            // Parent key must represent a directory, so use the directory key format
            Some(format!("{}{}", self.service, parent_path_vec.join("/")))
        } else {
            None // Root level file/dir in the service
        }
    }

    // Helper to get the display name (last path component)
    fn get_display_name(&self) -> String {
        self.path.last().cloned().unwrap_or_else(|| "/".to_string()) // FIXME Use "/" for potential root representation?
    }

    // --- format_tree_entry ---
    // Recursive function to format the output string and calculate total size
    // This is used for Single and Remote output modes.
    fn format_tree_entry(
        &self,
        indent_size: usize, // Spaces per level (e.g., 2)
        all_files: &HashMap<String, File>,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
    ) -> (String, u64) {
        let name = self.get_display_name();

        // Calculate indent based on the *absolute depth* (number of path components)
        // multiplied by the indent size per level.
        // self.path.len() is 1 for root items, 2 for their children, etc.
        // This makes root items have indent_size * 1 spaces, children indent_size * 2, etc.
        let indent = " ".repeat(indent_size * self.path.len());

        let starter = if self.is_dir { folder_icon } else { file_icon }; // Icons

        // Calculate size (recursive sum for dirs, own size for files)
        let mut total_size: u64 = if self.size >= 0 { self.size as u64 } else { 0 };
        let mut children_output = Vec::new();

        // Sort children: Files first, then folders, then alphabetically.
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by(|a_key, b_key| {
            match (all_files.get(*a_key), all_files.get(*b_key)) {
                (Some(a), Some(b)) => a
                    .is_dir // Sort by is_dir ascending (false=file, true=dir)
                    .cmp(&b.is_dir) // Files (false) come before Dirs (true)
                    .then_with(|| a.get_display_name().cmp(&b.get_display_name())), // Then alpha
                (None, Some(_)) => Ordering::Greater, // Handle missing keys (shouldn't happen ideally)
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        });

        // Recursively format children
        for key in sorted_children_keys {
            if let Some(child_file) = all_files.get(key) {
                // Pass the same indent_size down
                let (child_str, child_size) = child_file.format_tree_entry(
                    indent_size,
                    all_files,
                    folder_icon,
                    file_icon,
                    size_icon,
                    date_icon,
                );
                children_output.push(child_str);
                if self.is_dir {
                    total_size += child_size; // Add child size to dir total
                }
            } else {
                eprintln!("Warning: Child key '{}' not found in file map.", key);
            }
        }

        // For directories, size shown is the sum of its contents.
        // For files, size shown is its own size.
        let display_size = if self.is_dir {
            total_size
        } else {
            if self.size >= 0 {
                self.size as u64
            } else {
                0
            }
        };
        let size_str = human_bytes(display_size as f64);
        let modified_str = &self.modified; // Display modification time

        let children_str = children_output.join("\n");
        let line_sep = if children_output.is_empty() { "" } else { "\n" };

        // Format the current line using the calculated indent
        let entry_str = format!(
            "{}{} {} {size_icon} {} {date_icon} {}",
            indent, starter, name, size_str, modified_str
        );

        // Combine current entry with its children strings
        (
            format!("{}{}{}", entry_str, line_sep, children_str),
            total_size, // Return calculated total size (relevant for parent dirs)
        )
    }

    // Helper to calculate total size recursively without formatting strings
    fn calculate_recursive_size(&self, all_files: &HashMap<String, File>) -> u64 {
        if !self.is_dir {
            if self.size >= 0 {
                self.size as u64
            } else {
                0
            }
        } else {
            let mut total_size: u64 = 0; // Dirs themselves often have 0 or -1 size
            for key in &self.children_keys {
                if let Some(child_file) = all_files.get(key) {
                    total_size += child_file.calculate_recursive_size(all_files);
                }
            }
            total_size
        }
    }

    /// Helper function to format a list of direct children for Folder mode.
    fn format_direct_children_list(
        &self, // The parent File object (must be a directory)
        all_files: &HashMap<String, File>,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
    ) -> String {
        let mut children_lines = Vec::new();

        // Sort children: Files first, then folders, then alphabetically.
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by(|a_key, b_key| {
            match (all_files.get(*a_key), all_files.get(*b_key)) {
                (Some(a), Some(b)) => a
                    .is_dir
                    .cmp(&b.is_dir)
                    .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        });

        for key in sorted_children_keys {
            if let Some(child_file) = all_files.get(key) {
                let name = child_file.get_display_name();
                let starter = if child_file.is_dir {
                    folder_icon
                } else {
                    file_icon
                };

                // Calculate size (recursive for child dirs, own size for files) for display
                let display_size = child_file.calculate_recursive_size(all_files);
                let size_str = human_bytes(display_size as f64);
                let modified_str = &child_file.modified;

                // Format: Indent level 1, Icon, Name, Size, Date
                let line = format!(
                    "  {} {} {size_icon} {} {date_icon} {}",
                    starter, name, size_str, modified_str
                );
                children_lines.push(line);
            }
        }
        children_lines.join("\n")
    }

    /// Recursively writes directory structure and content files for Folder mode.
    fn write_fs_node_recursive(
        &self, // The File object for the current node (file or dir)
        parent_fs_path: &Path,
        all_files: &HashMap<String, File>,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
    ) -> io::Result<()> {
        if !self.is_dir {
            // Files are handled when their parent directory calls this function for its children.
            return Ok(());
        }

        // It's a directory, create it on the filesystem
        let current_fs_path = parent_fs_path.join(self.get_display_name());
        fs::create_dir_all(&current_fs_path)?;

        // Generate the content list for the _contents.txt file in this directory
        let contents_string = self.format_direct_children_list(
            all_files,
            folder_icon,
            file_icon,
            size_icon,
            date_icon,
        );

        // Write the _contents.txt file
        let contents_file_path = current_fs_path.join("files.txt");
        fs::write(&contents_file_path, contents_string)?;

        // Recursively call for child directories
        for child_key in &self.children_keys {
            if let Some(child_file) = all_files.get(child_key) {
                // Only recurse if the child is a directory itself
                if child_file.is_dir {
                    child_file.write_fs_node_recursive(
                        parent_fs_path, // The newly created directory is the parent for the next level
                        all_files,
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                    )?;
                }
            }
        }
        Ok(())
    }
}

// Internal struct for tracking duplicates
#[derive(Debug)]
struct DuplicateInfo {
    paths: Vec<String>, // List of full paths (service::path/to/file)
    size: u64,
}

// Files struct remains unchanged
#[derive(Debug)]
pub struct Files {
    pub files: HashMap<String, File>,
    duplicates: BTreeMap<Hashes, DuplicateInfo>,
    pub roots_by_service: HashMap<String, Vec<String>>,
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
        let mut service_keys_added_this_run = HashSet::new();
        let mut sorted_raw_files = raw_files;
        sorted_raw_files.sort(); // Process dirs first, shallow paths first

        for raw_file in sorted_raw_files {
            if raw_file.path.is_empty() {
                eprintln!("Warning: Skipping raw file with empty path: {:?}", raw_file);
                continue;
            }
            // Allow empty Name for root dir potentially
            if !raw_file.is_dir && raw_file.name.is_empty() {
                eprintln!("Warning: Skipping raw file with empty name: {:?}", raw_file);
                continue;
            }

            let file = File::from_raw(service_name.to_string(), &raw_file);
            let key = file.get_key();

            // Add file hashes to map for duplicate checking later
            if !file.is_dir {
                if let Some(hashes) = &file.hashes {
                    if file.size >= 0 {
                        // Only consider files with hashes and non-negative size
                        self.hash_map
                            .entry(hashes.clone())
                            .or_default()
                            .push(key.clone());
                    }
                }
            }
            // Store the file/dir; warn on overwrite (might indicate duplicate input)
            if self.files.insert(key.clone(), file.clone()).is_some() {
                eprintln!("Warning: Overwriting existing file key (potential duplicate entry in source JSON): {}", key);
            }
            service_keys_added_this_run.insert(key);
        }

        // Determine root keys for this service *after* processing all its files
        let service_root_keys: Vec<String> = service_keys_added_this_run
            .iter()
            .filter_map(|key| self.files.get(key)) // Get file from map
            .filter(|file| file.get_parent_key().is_none()) // Check if it's a root
            .map(|file| file.get_key()) // Get its key
            .collect();
        self.roots_by_service
            .insert(service_name.to_string(), service_root_keys); // Store roots

        println!(
            "Finished adding {} files for remote {}", // Note: self.files.len() is cumulative here
            service_keys_added_this_run.len(),        // Print count for this run
            service_name
        );
    }

    /// Builds the tree structure by linking parents and children
    pub fn build_tree(&mut self) {
        println!("Building tree structure...");
        let mut parent_child_map: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: Collect all parent-child relationships based on keys
        let all_keys: Vec<String> = self.files.keys().cloned().collect();

        // Find parent-child relationships
        for key in all_keys {
            if let Some(file) = self.files.get(&key) {
                if let Some(parent_key) = file.get_parent_key() {
                    // Ensure parent exists and is a directory
                    if let Some(parent_file) = self.files.get(&parent_key) {
                        if parent_file.is_dir {
                            parent_child_map
                                .entry(parent_key)
                                .or_default()
                                .insert(key.clone());
                        } else {
                            // This could happen if key generation logic is flawed or source data is inconsistent
                            eprintln!(
                                "Warning: Potential parent key '{}' exists but is not marked as a directory. Cannot assign child '{}'.",
                                parent_key, key
                            );
                        }
                    } else {
                        // This might happen if a parent directory wasn't listed by rclone lsjson
                        // or if key generation for parent/child is inconsistent.
                        eprintln!(
                            "Warning: Parent key '{}' not found for file '{}'. File might be orphaned in tree view.",
                            parent_key, key
                        );
                    }
                }
            }
        }
        // Update children_keys in parent File objects
        for (parent_key, children_keys) in parent_child_map {
            if let Some(parent_file) = self.files.get_mut(&parent_key) {
                // Ensure the parent is marked as a directory (sometimes rclone might not?)
                // parent_file.is_dir = true;
                parent_file.children_keys = children_keys;
            }
        }
        println!("Tree structure built.");
    }

    /// Finds duplicate files based on stored hashes.
    pub fn find_duplicates(&mut self) {
        println!("Finding duplicates based on hashes...");
        self.duplicates.clear(); // Reset duplicates map
        for (hash, keys) in &self.hash_map {
            if keys.len() > 1 {
                // Need at least 2 files for a duplicate set
                let mut paths = Vec::new();
                let mut size: Option<u64> = None;
                let mut valid_files_found = 0;
                for key in keys {
                    if let Some(file) = self.files.get(key) {
                        // Only consider actual files with non-negative size
                        if !file.is_dir && file.size >= 0 {
                            let service_and_path =
                                format!("{}:{}", file.service, file.path.join("/")); // Use colon separator for clarity
                            paths.push(service_and_path);
                            valid_files_found += 1;

                            // Store size (assume all files with same hash have same size)
                            // Take the first valid size found.
                            if size.is_none() {
                                size = Some(file.size as u64);
                            } else if size != Some(file.size as u64) {
                                // This would be unusual - same hash, different size reported by rclone?
                                eprintln!(
                                    "Warning: Files with same hash {:?} have different sizes reported: {} vs {} (Key: {})",
                                    hash,
                                    human_bytes(size.unwrap() as f64),
                                    human_bytes(file.size as f64),
                                    key
                                );
                                // FIXME Decide how to handle this - maybe skip this hash set? Or just report?
                                // For now, we'll keep the first size found.
                            }
                        }
                    }
                }
                // Store if we found multiple valid files with a consistent size
                if valid_files_found > 1 && size.is_some() {
                    paths.sort(); // Sort paths for consistent report output
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

    /// Calculates the total size and size per service.
    /// Returns: (Grand Total Size, Map<ServiceName, ServiceTotalSize>)
    fn calculate_all_sizes(&self) -> (u64, HashMap<String, u64>) {
        let mut grand_total_size: u64 = 0;
        let mut service_sizes: HashMap<String, u64> = HashMap::new();

        for service_name in self.roots_by_service.keys() {
            let mut service_total_size: u64 = 0;
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            for root_key in &root_keys {
                if let Some(root_file) = self.files.get(root_key) {
                    // Use the recursive size calculation helper
                    service_total_size += root_file.calculate_recursive_size(&self.files);
                }
            }
            grand_total_size += service_total_size;
            service_sizes.insert(service_name.clone(), service_total_size);
        }
        (grand_total_size, service_sizes)
    }

    /// Generates the formatted tree string for all services combined.
    /// Returns: Formatted Tree String
    fn generate_full_tree_string(
        &self,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
        service_sizes: &HashMap<String, u64>, // Pass in calculated sizes
    ) -> String {
        let mut final_text: Vec<String> = Vec::new();

        // Sort services alphabetically
        let mut sorted_services: Vec<&String> = self.roots_by_service.keys().collect();
        sorted_services.sort();

        for service_name in sorted_services {
            let service_total_size = service_sizes.get(service_name).cloned().unwrap_or(0);
            let mut service_entries_lines: Vec<String> = Vec::new();

            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            // Sort root keys: Files first, then Folders, then alphabetically
            let mut sorted_root_keys = root_keys;
            sorted_root_keys.sort_by(|a_key, b_key| {
                match (self.files.get(a_key), self.files.get(b_key)) {
                    (Some(a), Some(b)) => a
                        .is_dir // Sort by is_dir ascending (false=file, true=dir)
                        .cmp(&b.is_dir) // Files (false) come before Dirs (true)
                        .then_with(|| a.get_display_name().cmp(&b.get_display_name())), // Then alpha
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                }
            });

            // Generate tree strings for each root using format_tree_entry
            for root_key in &sorted_root_keys {
                if let Some(root_file) = self.files.get(root_key) {
                    let indent_size_per_level = 2;
                    // format_tree_entry calculates its own subtree size, but we ignore it here
                    // as we already have the total service size.
                    let (entry_str, _) = root_file.format_tree_entry(
                        indent_size_per_level,
                        &self.files,
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                    );
                    service_entries_lines.push(entry_str);
                } else {
                    eprintln!(
                        "Warning: Root key '{}' not found for service '{}'.",
                        root_key, service_name
                    );
                }
            }

            // Add service header (no indent)
            final_text.push(format!(
                "{} {}: {}",
                remote_icon,
                service_name,
                human_bytes(service_total_size as f64)
            ));
            // Add the collected (and now correctly indented) tree lines
            final_text.push(service_entries_lines.join("\n"));
            final_text.push("".to_string()); // Blank line between services
        }

        final_text.join("\n")
    }

    /// Generates the formatted tree string for a single service.
    /// Used for OutputDivisionMode::Remote.
    /// Returns: (Formatted Tree String for the service, Calculated Total Size for the service)
    fn generate_service_tree_string(
        &self,
        service_name: &str,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
    ) -> (String, u64) {
        let mut service_total_size: u64 = 0;
        let mut service_entries_lines: Vec<String> = Vec::new();

        let root_keys = self
            .roots_by_service
            .get(service_name)
            .cloned()
            .unwrap_or_default();

        // Sort root keys: Files first, then Folders, then alphabetically
        let mut sorted_root_keys = root_keys;
        sorted_root_keys.sort_by(|a_key, b_key| {
            match (self.files.get(a_key), self.files.get(b_key)) {
                (Some(a), Some(b)) => a
                    .is_dir
                    .cmp(&b.is_dir)
                    .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        });

        // Generate tree strings for each root using format_tree_entry
        for root_key in &sorted_root_keys {
            if let Some(root_file) = self.files.get(root_key) {
                let indent_size_per_level = 2;
                let (entry_str, entry_calculated_size) = root_file.format_tree_entry(
                    indent_size_per_level,
                    &self.files,
                    folder_icon,
                    file_icon,
                    size_icon,
                    date_icon,
                );
                service_entries_lines.push(entry_str);
                service_total_size += entry_calculated_size; // Accumulate size
            } else {
                eprintln!(
                    "Warning: Root key '{}' not found for service '{}'.",
                    root_key, service_name
                );
            }
        }

        // Format the final string for this service
        let header = format!(
            "{} {}: {}",
            remote_icon,
            service_name,
            human_bytes(service_total_size as f64)
        );
        let body = service_entries_lines.join("\n");

        (format!("{}\n{}", header, body), service_total_size)
    }

    /// Generates the formatted duplicates report string.
    pub fn generate_duplicates_output(&self) -> String {
        if self.duplicates.is_empty() {
            return "No duplicate files found based on available hashes.".to_string();
        }

        // Sort duplicates by potential savings, size, count
        let mut sorted_duplicates: Vec<(&Hashes, &DuplicateInfo)> =
            self.duplicates.iter().collect();
        sorted_duplicates.sort_by(|a, b| {
            let wasted_a = a.1.size * (a.1.paths.len().saturating_sub(1)) as u64;
            let wasted_b = b.1.size * (b.1.paths.len().saturating_sub(1)) as u64;
            wasted_b
                .cmp(&wasted_a) // Descending wasted space
                .then_with(|| b.1.size.cmp(&a.1.size)) // Then desc size
                .then_with(|| b.1.paths.len().cmp(&a.1.paths.len())) // Then desc count
        });

        let mut lines: Vec<String> = Vec::new();
        // lines.push("--- Duplicate Files Report ---".to_string());

        let mut total_potential_savings: u64 = 0;
        let mut total_duplicate_size: u64 = 0; // Total size occupied by all duplicate files

        for (_hashes, info) in &sorted_duplicates {
            let num_files = info.paths.len();
            if num_files > 1 {
                // Ensure it's actually a set of duplicates
                let current_duplicate_set_total_size = info.size * num_files as u64;
                let potential_saving = info.size * (num_files.saturating_sub(1)) as u64;
                total_duplicate_size += current_duplicate_set_total_size;
                total_potential_savings += potential_saving;
            }
        }

        // Add summary information at the top
        lines.push(format!(
            "Total size occupied by all files identified as duplicates: {}",
            human_bytes(total_duplicate_size as f64)
        ));
        lines.push(format!(
            "Found {} sets of files with matching hashes.",
            sorted_duplicates.len()
        ));
        lines.push(format!(
            "Total potential disk space saving by removing duplicates (keeping one copy of each): {}",
             human_bytes(total_potential_savings as f64)
        ));

        for (hashes, info) in sorted_duplicates {
            let num_duplicates = info.paths.len();
            let potential_saving = info.size * (num_duplicates.saturating_sub(1)) as u64;

            lines.push(format!(
                "\nDuplicates found with size: {} ({} files, potential saving: {})",
                human_bytes(info.size as f64),
                num_duplicates,
                human_bytes(potential_saving as f64)
            ));
            // List paths
            for path_key in &info.paths {
                lines.push(format!("  - {}", path_key));
            }
            // List hashes
            let mut hash_parts = Vec::new();
            if let Some(h) = &hashes.md5 {
                hash_parts.push(format!("MD5: {}", h));
            }
            if let Some(h) = &hashes.sha1 {
                hash_parts.push(format!("SHA-1: {}", h));
            }
            if let Some(h) = &hashes.sha256 {
                hash_parts.push(format!("SHA-256: {}", h));
            }
            if let Some(h) = &hashes.dropbox {
                hash_parts.push(format!("DropboxHash: {}", h));
            }
            if let Some(h) = &hashes.quickxor {
                hash_parts.push(format!("QuickXorHash: {}", h));
            }
            if !hash_parts.is_empty() {
                lines.push(format!("  Matching Hashes: {}", hash_parts.join(", ")));
            }
        }
        // Add summary footer (already included total potential savings at the top)
        // lines.push("\n--- End of Report ---".to_string());
        lines.join("\n")
    }

    // Helper function to access roots_by_service keys
    pub fn get_service_names(&self) -> Vec<String> {
        self.roots_by_service.keys().cloned().collect()
    }
}

// --- Public Functions ---

// Helper to run a command
pub fn run_command(executable: &str, args: &[&str]) -> Result<Output, io::Error> {
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

/// Parses the JSON output from `rclone lsjson`.
/// Returns a Vec of RawFile objects or a JSON parsing error.
pub fn parse_rclone_lsjson(json_data: &str) -> Result<Vec<RawFile>, serde_json::Error> {
    serde_json::from_str(json_data)
}

/// Processes the aggregated file data, builds the tree, finds duplicates (if requested),
/// and writes the reports (tree/files, duplicates, size_used) to disk based on the chosen mode.
pub fn generate_reports(
    files_data: &mut Files,
    output_division_mode: OutputDivisionMode, // New parameter
    enable_duplicates_report: bool,
    output_dir: &Path,        // Changed from tree_output_path to directory path
    tree_base_filename: &str, // Base name for single file mode, e.g., "files.txt"
    duplicates_output_filename: &str,
    size_output_filename: &str,
    folder_icon: &str,
    file_icon: &str,
    size_icon: &str,
    date_icon: &str,
    remote_icon: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating standard reports (Mode: {:?})...",
        output_division_mode
    );

    // Ensure output directory exists (should be created by main, but double-check)
    fs::remove_dir_all(output_dir)?;
    fs::create_dir_all(output_dir)?;

    // --- 1. Build parent-child links ---
    files_data.build_tree();

    // --- 2. Find and report duplicates (if enabled) ---
    // Duplicates report is always a single file.
    let duplicates_output_path = output_dir.join(duplicates_output_filename);
    if enable_duplicates_report { // TODO run this in parallel
        files_data.find_duplicates();
        let duplicates_report_string = files_data.generate_duplicates_output();
        fs::write(&duplicates_output_path, duplicates_report_string)?;
        println!(
            "Duplicates report written to '{}'",
            duplicates_output_path.display()
        );
    } else {
        println!("Duplicate detection skipped.");
        if duplicates_output_path.exists() {
            let _ = fs::remove_file(&duplicates_output_path);
            println!(
                "Removed existing duplicates report file '{}'",
                duplicates_output_path.display()
            );
        }
    }

    // --- 3. Calculate sizes (needed for reports) ---
    let (grand_total_size, service_sizes) = files_data.calculate_all_sizes();

    // --- 4. Generate Size Report (always a single file) ---
    let size_output_path = output_dir.join(size_output_filename);
    let mut size_report_lines: Vec<String> = Vec::new();
    size_report_lines.push("".to_string()); // Blank line

    let mut sorted_service_names: Vec<&String> = service_sizes.keys().collect();
    sorted_service_names.sort();

    for service_name in sorted_service_names {
        if let Some(size) = service_sizes.get(service_name) {
            size_report_lines.push(format!(
                "{}: {} used",
                service_name,
                human_bytes(*size as f64)
            ));
        }
    }
    size_report_lines.push("".to_string()); // Separator
    size_report_lines.push(format!(
        "Total used size across all services (calculated from listings): {}",
        human_bytes(grand_total_size as f64)
    ));
    let size_report_string = size_report_lines.join("\n");
    fs::write(&size_output_path, &size_report_string)?;
    println!("Size report written to '{}'", size_output_path.display());

    // --- 5. Generate Tree/File Structure Report based on Mode ---
    match output_division_mode {
        OutputDivisionMode::Single => {
            let tree_report_string = files_data.generate_full_tree_string(
                folder_icon,
                file_icon,
                size_icon,
                date_icon,
                remote_icon,
                &service_sizes,
            );
            let tree_output_file_path = output_dir.join(tree_base_filename);
            fs::write(&tree_output_file_path, &tree_report_string)?;
            println!(
                "Tree report (single file) written to '{}'",
                tree_output_file_path.display()
            );
        }
        OutputDivisionMode::Remote => {
            // Regenerate sizes map if needed, though we have it from `calculate_all_sizes`
            let mut sorted_services: Vec<&String> = files_data.roots_by_service.keys().collect();
            sorted_services.sort();

            for service_name in sorted_services {
                // Generate string and size just for this service
                let (service_string, _service_calculated_size) = files_data
                    .generate_service_tree_string(
                        service_name,
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                        remote_icon,
                    );

                // Construct filename like "output_dir/remote_name.txt"
                let service_filename = format!("{}.txt", service_name);
                let service_output_path = output_dir.join(service_filename);
                fs::create_dir_all(&service_output_path)?;
                fs::write(&service_output_path, service_string)?;
                println!(
                    "  - Remote file list written to '{}'",
                    service_output_path.display()
                );
            }
            println!(
                "Tree report (per remote) written to directory '{}'",
                output_dir.display()
            );
        }
        OutputDivisionMode::Folder => {
            let mut sorted_services: Vec<&String> = files_data.roots_by_service.keys().collect();
            sorted_services.sort();

            for service_name in sorted_services {
                let service_base_path = output_dir.join(service_name);
                // No need to create dir here, write_fs_node_recursive handles it for roots too

                let root_keys = files_data
                    .roots_by_service
                    .get(service_name)
                    .cloned()
                    .unwrap_or_default();

                for root_key in &root_keys {
                    if let Some(root_file) = files_data.files.get(root_key) {
                        // The initial parent path is the service's base directory
                        root_file.write_fs_node_recursive(
                            &service_base_path.parent().unwrap_or(output_dir), // Pass parent dir
                            &files_data.files,
                            folder_icon,
                            file_icon,
                            size_icon,
                            date_icon,
                        )?;
                    }
                }
                println!(
                    "  - Folder structure for remote '{}' written.",
                    service_name
                );
            }
            println!(
                "Tree report (folder structure) written to directory '{}'",
                output_dir.display()
            );
        }
    }

    println!("Standard reports generation finished.");
    Ok(())
}

/// Generates the 'about' report by running `rclone about` for each remote.
pub fn generate_about_report(
    remote_names: &[String], // Use slice of strings
    rclone_executable: &str,
    common_rclone_args: &[String], // Use slice of strings
    about_output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating about report (checking remote sizes)...");
    let mut report_lines: Vec<(String, String)> = Vec::new(); // Store (name, formatted_line) for sorting

    // Variables to accumulate totals
    let mut grand_total_used: u64 = 0;
    let mut grand_total_free: u64 = 0;
    let mut grand_total_trashed: u64 = 0;
    // grand_total_total might be misleading if remotes have different quotas or are unlimited
    let mut remotes_with_data = 0; // Count remotes that successfully provided data

    for remote_name in remote_names {
        let remote_target = format!("{}:", remote_name);
        let mut about_args_owned: Vec<String> = common_rclone_args.to_vec(); // Clone common args

        about_args_owned.extend(vec![
            "about".to_string(),
            remote_target.clone(), // Add the specific remote
            "--json".to_string(),
        ]);

        // Convert Vec<String> to Vec<&str> for run_command
        let about_args_for_cmd: Vec<&str> = about_args_owned.iter().map(|s| s.as_str()).collect();

        match run_command(rclone_executable, &about_args_for_cmd) {
            Ok(output) => {
                if output.status.success() {
                    let json_string = String::from_utf8_lossy(&output.stdout);
                    match serde_json::from_str::<RcloneAboutInfo>(&json_string) {
                        Ok(about_info) => {
                            remotes_with_data += 1; // Count successful data retrieval

                            // Accumulate totals, handling Option<u64>
                            grand_total_used += about_info.used.unwrap_or(0);
                            grand_total_free += about_info.free.unwrap_or(0);
                            grand_total_trashed += about_info.trashed.unwrap_or(0);

                            let used_str = about_info
                                .used
                                .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                            let total_str = about_info
                                .total
                                .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                            let free_str = about_info
                                .free
                                .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                            // Conditional formatting for trashed size
                            let trashed_str = about_info.trashed.filter(|&v| v > 0).map_or_else(
                                || "".to_string(), // Don't show if zero or N/A
                                |v| format!(", Trashed={}", human_bytes(v as f64)),
                            );
                            // Conditional formatting for other size (less common)
                            let other_str = about_info.other.filter(|&v| v > 0).map_or_else(
                                || "".to_string(), // Don't show if zero or N/A
                                |v| format!(", Other={}", human_bytes(v as f64)),
                            );

                            // Format the line for the specific remote including conditional trashed/other
                            let line = format!(
                                "{}: Used={}, Free={}, Total={}{}{}",
                                remote_name, used_str, free_str, total_str, trashed_str, other_str
                            );
                            report_lines.push((remote_name.clone(), line));
                        }
                        Err(e) => {
                            let line =
                                format!("{}: Error parsing 'about' JSON - {}", remote_name, e);
                            report_lines.push((remote_name.clone(), line));
                            eprintln!(
                                "  Failed to parse 'rclone about' JSON for {}: {}",
                                remote_name, e
                            );
                        }
                    }
                } else {
                    let line = format!(
                        "{}: Failed to get 'about' info (Command failed with status {})",
                        remote_name, output.status
                    );
                    report_lines.push((remote_name.clone(), line));
                    // stderr already printed by run_command
                }
            }
            Err(e) => {
                let line = format!("{}: Failed to execute 'rclone about' - {}", remote_name, e);
                report_lines.push((remote_name.clone(), line));
                eprintln!(
                    "  Failed to execute 'rclone about' for {}: {}",
                    remote_name, e
                );
            }
        }
    }

    // Sort lines alphabetically by remote name
    report_lines.sort_by(|a, b| a.0.cmp(&b.0));

    // Prepare final output string
    let mut final_report_lines: Vec<String> = Vec::new();
    final_report_lines.extend(report_lines.into_iter().map(|(_, line)| line)); // Extract just the formatted lines

    // Add Grand Total line if any remotes reported data
    if remotes_with_data > 0 {
        final_report_lines.push("".to_string());
        let grand_total_line = format!(
            "Grand Total ({} remotes): Used={}, Free={}, Trashed={}, Total Capacity={}",
            remotes_with_data,
            human_bytes(grand_total_used as f64),
            human_bytes(grand_total_free as f64),
            human_bytes(grand_total_trashed as f64),
            human_bytes((grand_total_used + grand_total_free) as f64),
        );
        final_report_lines.push(grand_total_line);
    } else {
        final_report_lines.push("".to_string());
        final_report_lines.push("Grand Total: No data available from any remote.".to_string());
    }

    // final_report_lines.push("--- End of Report ---".to_string());

    let final_report_string = final_report_lines.join("\n");

    fs::write(about_output_path, final_report_string)?;
    println!("About report written to '{}'", about_output_path);

    Ok(())
}
