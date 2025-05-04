use clap::ValueEnum;
use human_bytes::human_bytes;
use rayon::prelude::*; // Import for parallel sort
use serde::{Deserialize, Serialize};
use std::cell::RefCell; // Used for caching size calculation temporarily
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::process::{Command, Output, Stdio};

const MODE_REMOTES_SERVICE_PREFIX: &str = "_";
const NO_EXTENSION_KEY: &str = "[no extension]";

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "kebab-case")]
pub enum OutputMode {
    Single,
    Remotes,
    Folders,
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
pub struct RcloneAboutInfo {
    total: Option<u64>,
    used: Option<u64>,
    trashed: Option<u64>,
    other: Option<u64>,
    free: Option<u64>,
}

// Helper type for parallel about processing results
pub type AboutResult = Result<(String, RcloneAboutInfo), (String, String)>;

fn get_name_and_extension(path: &str) -> (String, Option<String>) {
    let p = Path::new(path);
    let name = p
        .file_stem()
        .map_or_else(|| path.to_string(), |s| s.to_string_lossy().into_owned());
    // Get extension and convert to lowercase for consistent grouping
    let ext = p.extension().map(|s| s.to_string_lossy().to_lowercase());
    (name, ext)
}

#[derive(Debug, Clone)]
pub struct File {
    service: String,   // The original remote name
    pub ext: String,   // Keep public for extension report access
    path: Vec<String>, // Path components (e.g., ["folder1", "file.txt"])
    modified: String,
    size: i64,                      // Keep as i64 to handle potential -1 from rclone
    pub is_dir: bool,               // Keep public for reporting access
    children_keys: HashSet<String>, // Keys of direct children
    hashes: Option<Hashes>,
}

impl File {
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
            // For files, extract from Name field (lowercase extension)
            get_name_and_extension(&raw_file.name)
        };
        let mut final_path = path_components;
        if !raw_file.is_dir && ext_opt.is_some() {
            if let Some(last) = final_path.last_mut() {
                // Basic check to avoid replacing a component like 'archive.tar.gz' with 'archive.tar'
                if !last.ends_with(&format!(".{}", ext_opt.as_deref().unwrap_or(""))) {
                    if *last != name_part {
                        if !last.ends_with(&format!(".{}", ext_opt.as_deref().unwrap_or(""))) {
                            *last = name_part; // Apply if last component doesn't look like it includes the ext
                        }
                    }
                }
            }
        }

        Self {
            service,
            ext: ext_opt.unwrap_or_default(), // Use empty string if no extension
            path: final_path,                 // Contains components like ["dir1", "subdir", "file"]
            modified: raw_file.mod_time.clone(),
            size: raw_file.size,
            is_dir: raw_file.is_dir,
            children_keys: HashSet::new(),
            hashes: raw_file.hashes.clone(),
        }
    }

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
        self.path
            .last()
            .cloned()
            .unwrap_or_else(|| self.service.clone())
    }

    // --- format_tree_entry ---
    // Recursive function to format the output string and calculate total size
    // Formats the output string, retrieves size from cache.
    fn format_tree_entry(
        &self,
        indent_size: usize,
        all_files: &HashMap<String, File>,
        size_cache: &HashMap<String, u64>,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
    ) -> String {
        let name = self.get_display_name();
        let key = self.get_key(); // Get key to lookup in cache

        let indent_level = self.path.len();
        let indent = " ".repeat(indent_size * indent_level);
        let starter = if self.is_dir { folder_icon } else { file_icon };

        // Retrieve pre-calculated size from cache
        let display_size = size_cache.get(&key).cloned().unwrap_or(0); // Default to 0 if somehow missing
        let size_str = human_bytes(display_size as f64);
        let modified_str = &self.modified;

        let mut children_output = Vec::new();

        // Sort children: Files first, then Dirs, then alphabetically.
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by(|a_key, b_key| {
            match (all_files.get(*a_key), all_files.get(*b_key)) {
                (Some(a), Some(b)) => a
                    .is_dir
                    .cmp(&b.is_dir)
                    // .reverse()
                    .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        });

        // Recursively format children, passing the cache down
        for child_key in sorted_children_keys {
            if let Some(child_file) = all_files.get(child_key) {
                let child_str = child_file.format_tree_entry(
                    indent_size,
                    all_files,
                    size_cache,
                    folder_icon,
                    file_icon,
                    size_icon,
                    date_icon,
                );
                children_output.push(child_str);
            } else {
                eprintln!("Warning: Child key '{}' not found in file map.", child_key);
            }
        }

        let children_str = children_output.join("\n");
        let line_sep = if children_output.is_empty() { "" } else { "\n" };

        let entry_str = format!(
            "{}{} {} {size_icon} {} {date_icon} {}",
            indent, starter, name, size_str, modified_str
        );

        format!("{}{}{}", entry_str, line_sep, children_str)
    }

    /// Helper function to format a list of direct children for Folder mode (Uses size_cache).
    fn format_direct_children_list(
        &self,
        all_files: &HashMap<String, File>,
        size_cache: &HashMap<String, u64>,
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

                // Retrieve size from cache using child's key
                let child_key = child_file.get_key();
                let display_size = size_cache.get(&child_key).cloned().unwrap_or(0); // Default 0
                let size_str = human_bytes(display_size as f64);
                let modified_str = &child_file.modified;

                let line = format!(
                    "  {} {} {size_icon} {} {date_icon} {}",
                    starter, name, size_str, modified_str
                );
                children_lines.push(line);
            }
        }
        children_lines.join("\n")
    }

    /// Recursively writes directory structure and content files for Folder mode (Uses size_cache).
    fn write_fs_node_recursive(
        &self,
        parent_fs_path: &Path,
        all_files: &HashMap<String, File>,
        size_cache: &HashMap<String, u64>, // Added size cache parameter
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        folder_content_filename: &str,
    ) -> io::Result<()> {
        if !self.is_dir {
            return Ok(());
        }

        let current_fs_path = parent_fs_path.join(self.get_display_name());
        fs::create_dir_all(&current_fs_path)?; // Ensure this directory exists

        // Generate the content list using the cache
        let contents_string = self.format_direct_children_list(
            all_files,
            size_cache,
            folder_icon,
            file_icon,
            size_icon,
            date_icon,
        );

        let contents_file_path = current_fs_path.join(folder_content_filename);
        fs::write(&contents_file_path, contents_string)?;

        // Recursively call for child DIRECTORIES only, passing the cache
        for child_key in &self.children_keys {
            if let Some(child_file) = all_files.get(child_key) {
                if child_file.is_dir {
                    child_file.write_fs_node_recursive(
                        current_fs_path.as_path(),
                        all_files,
                        size_cache, // Pass cache
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                        folder_content_filename,
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
    paths: Vec<String>, // List of full paths (service:path/to/file)
    size: u64,
}

// Helper struct for extension report
#[derive(Debug, Clone)]
struct ExtensionInfo {
    extension: String,
    count: u64,
    total_size: u64,
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
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            duplicates: BTreeMap::new(),
            roots_by_service: HashMap::new(),
            hash_map: HashMap::new(),
        }
    }

    pub fn add_remote_files(&mut self, service_name: &str, raw_files: Vec<RawFile>) {
        println!("Adding files for service: {}", service_name);
        let mut service_keys_added_this_run = HashSet::new();
        let mut sorted_raw_files = raw_files;
        sorted_raw_files.sort();

        for raw_file in sorted_raw_files {
            // Skip entries that are clearly invalid or represent the root directory itself
            // which rclone lsjson sometimes includes with empty Path/Name.
            if raw_file.path.is_empty() && raw_file.name.is_empty() && raw_file.is_dir {
                continue;
            }
            if raw_file.path.is_empty() {
                eprintln!("Warning: Skipping raw file with empty path: {:?}", raw_file);
                continue;
            }
            // Allow empty Name for directories if Path is set (e.g., top-level dir)
            if !raw_file.is_dir && raw_file.name.is_empty() {
                eprintln!("Warning: Skipping raw file with empty name: {:?}", raw_file);
                continue;
            }

            let file = File::from_raw(service_name.to_string(), &raw_file);
            let key = file.get_key();

            // Add file hashes to map for duplicate checking later
            if !file.is_dir {
                if let Some(hashes) = &file.hashes {
                    // Only consider files with at least one hash type present
                    // (Checking for None Option is done by let Some)
                    // Further filter by specific hash types if needed (e.g., require SHA1 or MD5)
                    if file.size >= 0 {
                        // Only consider files with non-negative size
                        self.hash_map
                            .entry(hashes.clone())
                            .or_default()
                            .push(key.clone());
                    }
                }
            }

            // Store the file/dir; warn on overwrite (might indicate duplicate input)
            if self.files.insert(key.clone(), file.clone()).is_some() {
                if let Some(existing_file) = self.files.get(&key) {
                    if existing_file.is_dir != file.is_dir {
                        eprintln!("Warning: Overwriting existing key '{}' with different type (is_dir: {} -> {}). Check rclone output.", key, existing_file.is_dir, file.is_dir);
                    }
                }
                // Re-insert the new file regardless of warning
                self.files.insert(key.clone(), file);
            }
            service_keys_added_this_run.insert(key);
        }

        // Determine root keys for this service after processing all its files
        let service_root_keys: Vec<String> = service_keys_added_this_run
            .iter()
            .filter_map(|key| self.files.get(key))
            .filter(|file| file.get_parent_key().is_none())
            .map(|file| file.get_key())
            .collect();
        self.roots_by_service
            .insert(service_name.to_string(), service_root_keys);

        println!(
            "Finished adding {} files/dirs for remote {}",
            service_keys_added_this_run.len(),
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
                                .entry(parent_key.clone())
                                .or_default()
                                .insert(key.clone());
                        } else {
                            // This could happen if key generation logic is flawed or source data is inconsistent
                            eprintln!(
                                "Warning: Potential parent key '{}' for child '{}' exists but is not marked as a directory. Cannot assign child.",
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
                        // We might want to add it as a root for its service if it's orphaned?
                        // Or just let it be unlinked. For now, just warn.
                    }
                }
                // If get_parent_key returned None, it's already considered a root (handled in add_remote_files)
            }
        }

        // Second pass: Update children_keys in parent File objects
        for (parent_key, children_keys) in parent_child_map {
            if let Some(parent_file) = self.files.get_mut(&parent_key) {
                // We already checked parent_file.is_dir in the first pass
                parent_file.children_keys = children_keys;
            }
        }
        println!("Tree structure built.");
    }

    /// Finds duplicate files based on stored hashes.
    pub fn find_duplicates(&mut self) {
        println!("Finding duplicates based on hashes...");
        self.duplicates.clear();
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
                                format!("{}:{}", file.service, file.path.join("/"));
                            paths.push(service_and_path);
                            valid_files_found += 1;
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
                    paths.sort();
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

    /// Calculates sizes recursively, populating a cache.
    /// Returns: (Grand Total Size, Map<ServiceName, ServiceTotalSize>, Map<FileKey, CalculatedSize>)
    fn calculate_all_sizes_with_cache(&self) -> (u64, HashMap<String, u64>, HashMap<String, u64>) {
        let mut grand_total_size: u64 = 0;
        let mut service_sizes: HashMap<String, u64> = HashMap::new();
        // Use RefCell for interior mutability of the cache within the recursive helper
        let size_cache = RefCell::new(HashMap::new());

        // Recursive helper function using the RefCell cache
        fn calculate_recursive_and_cache(
            key: &str,
            all_files: &HashMap<String, File>,
            cache: &RefCell<HashMap<String, u64>>,
        ) -> u64 {
            // Check cache first (read borrow)
            if let Some(&cached_size) = cache.borrow().get(key) {
                return cached_size;
            }

            // Not in cache, calculate it
            let calculated_size = if let Some(file) = all_files.get(key) {
                if !file.is_dir {
                    // File: use its size (handle -1)
                    if file.size >= 0 {
                        file.size as u64
                    } else {
                        0
                    }
                } else {
                    // Directory: sum sizes of children recursively
                    let mut total_size: u64 = 0;
                    for child_key in &file.children_keys {
                        // Recursive call
                        total_size += calculate_recursive_and_cache(child_key, all_files, cache);
                    }
                    total_size
                }
            } else {
                eprintln!("Warning: Key '{}' not found during size calculation.", key);
                0 // Return 0 if key is somehow missing
            };

            // Store in cache (write borrow) before returning
            cache.borrow_mut().insert(key.to_string(), calculated_size);
            calculated_size
        }

        // Iterate through roots of each service to start calculations
        for service_name in self.roots_by_service.keys() {
            let mut service_total_size: u64 = 0;
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            for root_key in &root_keys {
                // Call the recursive helper which populates the cache
                service_total_size +=
                    calculate_recursive_and_cache(root_key, &self.files, &size_cache);
            }
            grand_total_size += service_total_size;
            service_sizes.insert(service_name.clone(), service_total_size);
        }

        // Return totals and the final populated cache
        (grand_total_size, service_sizes, size_cache.into_inner())
    }

    /// Generates the formatted tree string for all services combined (Uses size_cache).
    fn generate_full_tree_string(
        &self,
        size_cache: &HashMap<String, u64>, // Added size cache parameter
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
        service_sizes: &HashMap<String, u64>,
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
                        .is_dir
                        .cmp(&b.is_dir)
                        .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                }
            });

            for root_key in &sorted_root_keys {
                if let Some(root_file) = self.files.get(root_key) {
                    let indent_size_per_level = 2;
                    // Use format_tree_entry which now uses the cache
                    let entry_str = root_file.format_tree_entry(
                        indent_size_per_level,
                        &self.files,
                        size_cache,
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

            final_text.push(format!(
                "{} {}: {}",
                remote_icon,
                service_name,
                human_bytes(service_total_size as f64)
            ));
            final_text.push(service_entries_lines.join("\n"));
            final_text.push("".to_string());
        }

        final_text.join("\n")
    }

    /// Generates the formatted tree string for a single service (Uses size_cache).
    fn generate_service_tree_string(
        &self,
        service_name: &str,
        size_cache: &HashMap<String, u64>,
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
    ) -> (String, u64) {
        // FIXME Still returns total size for consistency if needed upstream
        let mut service_total_size: u64 = 0; // Recalculate here from cache for verification/return
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

        for root_key in &sorted_root_keys {
            if let Some(root_file) = self.files.get(root_key) {
                // Add this root's cached size to the total
                service_total_size += size_cache.get(root_key).cloned().unwrap_or(0);

                let indent_size_per_level = 2;
                let entry_str = root_file.format_tree_entry(
                    indent_size_per_level,
                    &self.files,
                    size_cache,
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

        let header = format!(
            "{} {}: {}",
            remote_icon,
            service_name,
            human_bytes(service_total_size as f64) // Display calculated total
        );
        let body = service_entries_lines.join("\n");

        (format!("{}\n{}", header, body), service_total_size)
    }

    /// Generates the formatted duplicates report string (Uses parallel sort).
    pub fn generate_duplicates_output(&self) -> String {
        if self.duplicates.is_empty() {
            return "No duplicate files found based on available hashes.".to_string();
        }

        let mut sorted_duplicates: Vec<(&Hashes, &DuplicateInfo)> =
            self.duplicates.iter().collect();

        // Use parallel sort
        sorted_duplicates.par_sort_unstable_by(|a, b| {
            let wasted_a = a.1.size * (a.1.paths.len().saturating_sub(1)) as u64;
            let wasted_b = b.1.size * (b.1.paths.len().saturating_sub(1)) as u64;
            wasted_b
                .cmp(&wasted_a)
                .then_with(|| b.1.size.cmp(&a.1.size))
                .then_with(|| b.1.paths.len().cmp(&a.1.paths.len()))
        });

        let mut lines: Vec<String> = Vec::new();
        let mut total_potential_savings: u64 = 0;
        let mut total_duplicate_size: u64 = 0;

        for (_hashes, info) in &sorted_duplicates {
            let num_files = info.paths.len();
            if num_files > 1 {
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
        lines.join("\n")
    }

    /// Generates the formatted extensions report string (Uses parallel sort, ordered by size).
    pub fn generate_extensions_report(&self) -> String {
        println!("Generating extensions report...");
        let mut extensions_map: HashMap<String, (u64, u64)> = HashMap::new();
        let mut total_file_count: u64 = 0;
        let mut total_files_size: u64 = 0;

        for file in self.files.values() {
            if !file.is_dir && file.size >= 0 {
                total_file_count += 1;
                let file_size = file.size as u64;
                total_files_size += file_size;
                let ext_key = if file.ext.is_empty() {
                    NO_EXTENSION_KEY.to_string()
                } else {
                    file.ext.clone()
                };
                let entry = extensions_map.entry(ext_key).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += file_size;
            }
        }

        if extensions_map.is_empty() {
            return "No files found to generate extensions report.".to_string();
        }

        // Convert map to Vec for sorting
        let mut sorted_extensions: Vec<ExtensionInfo> = extensions_map
            .into_iter()
            .map(|(ext, (count, size))| ExtensionInfo {
                extension: ext,
                count,
                total_size: size,
            })
            .collect();

        // Use parallel sort, prioritizing Size (desc), then Count (desc), then Name (asc)
        sorted_extensions.par_sort_unstable_by(|a, b| {
            b.total_size // Primary: Size Desc
                .cmp(&a.total_size)
                .then_with(|| b.count.cmp(&a.count)) // Secondary: Count Desc
                .then_with(|| a.extension.cmp(&b.extension)) // Tertiary: Name Asc
        });

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!(
            "Total Files Found: {} (Total Size: {})",
            total_file_count,
            human_bytes(total_files_size as f64)
        ));
        lines.push("-------------------------------------".to_string());
        lines.push(format!(
            "{:<15} | {:>15} | {:>12} | {:>8} | {:>8}", // Order columns visually: Ext, Size, Count
            "Extension", "Total Size", "File Count", "% Size", "% Count"
        ));
        lines.push(
            "--------------------------------------------------------------------".to_string(),
        );

        for info in sorted_extensions {
            let count_percent = if total_file_count > 0 {
                (info.count as f64 / total_file_count as f64) * 100.0
            } else {
                0.0
            };
            let size_percent = if total_files_size > 0 {
                (info.total_size as f64 / total_files_size as f64) * 100.0
            } else {
                0.0
            };
            let ext_display = if info.extension == NO_EXTENSION_KEY {
                info.extension.clone()
            } else {
                format!(".{}", info.extension)
            };

            // Adjust format string to match new column order
            lines.push(format!(
                "{:<15} | {:>15} | {:>12} | {:>7.2}% | {:>7.2}%",
                ext_display,
                human_bytes(info.total_size as f64), // Size first
                info.count,                          // Then Count
                size_percent,                        // Then Size %
                count_percent                        // Then Count %
            ));
        }
        lines.join("\n")
    }

    pub fn get_service_names(&self) -> Vec<String> {
        self.roots_by_service.keys().cloned().collect()
    }
}

// --- Public Functions ---
pub fn run_command(executable: &str, args: &[&str]) -> Result<Output, io::Error> {
    let args_display = args
        .iter()
        .map(|&a| {
            if a.contains(' ') { format!("\"{}\"", a) } else { a.to_string() }
        })
        .collect::<Vec<_>>()
        .join(" ");
    // Reduced noisy printing during parallel runs, log start from caller if needed
    // println!("> Running: {} {}", executable, args_display);
    let output = Command::new(executable)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();
    match output {
        Ok(ref out) => {
            if !out.status.success() {
                // eprintln!("  Command failed with status: {}", out.status); // Reduced verbosity
                let stderr_str = String::from_utf8_lossy(&out.stderr);
                if !stderr_str.is_empty() {
                    // Log errors centrally after parallel execution if possible
                    eprintln!(
                        "  Command '{} {}' failed with stderr:\n---\n{}\n---",
                        executable,
                        args_display,
                        stderr_str.trim()
                    );
                } else {
                    eprintln!(
                        "  Command '{} {}' failed with status code {}. No stderr output.",
                        executable, args_display, out.status
                    );
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

/// Processes the aggregated file data, builds the tree, finds duplicates, calculates sizes+cache
/// and writes the reports to disk based on the chosen mode.
#[allow(clippy::too_many_arguments)]
pub fn generate_reports(
    files_data: &mut Files,
    output_division_mode: OutputMode,
    enable_duplicates_report: bool,
    enable_extensions_report: bool,
    output_dir: &Path,
    tree_base_filename: &str,
    folder_content_filename: &str,
    duplicates_output_filename: &str,
    size_output_filename: &str,
    extensions_output_filename: &str,
    folder_icon: &str,
    file_icon: &str,
    size_icon: &str,
    date_icon: &str,
    remote_icon: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating reports (Mode: {:?})...", output_division_mode);
    fs::create_dir_all(output_dir)?;

    // --- 1. Build parent-child links ---
    files_data.build_tree();

    // --- 2. Find and report duplicates (if enabled, uses parallel sort) ---
    let duplicates_output_path = output_dir.join(duplicates_output_filename);
    if enable_duplicates_report {
        files_data.find_duplicates();
        let duplicates_report_string = files_data.generate_duplicates_output(); // Now uses par_sort
        fs::write(&duplicates_output_path, duplicates_report_string)?;
        println!(
            "Duplicates report written to '{}'",
            duplicates_output_path.display()
        );
    } else {
        println!("Duplicate detection skipped.");
        if duplicates_output_path.exists() {
            let _ = fs::remove_file(&duplicates_output_path);
        }
    }

    // --- 2b. Generate and report extensions (if enabled, uses parallel sort by size) ---
    let extensions_output_path = output_dir.join(extensions_output_filename);
    if enable_extensions_report {
        let extensions_report_string = files_data.generate_extensions_report(); // Now uses par_sort
        fs::write(&extensions_output_path, extensions_report_string)?;
        println!(
            "Extensions report written to '{}'",
            extensions_output_path.display()
        );
    } else {
        println!("Extensions report skipped.");
        if extensions_output_path.exists() {
            let _ = fs::remove_file(&extensions_output_path);
        }
    }

    // --- 3. Calculate sizes and populate cache (runs once) ---
    println!("Calculating sizes and populating cache...");
    let (grand_total_size, service_sizes, size_cache) = files_data.calculate_all_sizes_with_cache();
    println!("Size calculation complete.");

    // --- 4. Generate Size Report (using calculated totals) ---
    let size_output_path = output_dir.join(size_output_filename);
    let mut size_report_lines: Vec<String> = Vec::new();
    size_report_lines.push(format!(
        "Total used size across all services (calculated from listings): {}",
        human_bytes(grand_total_size as f64)
    ));
    size_report_lines.push("".to_string());

    let mut sorted_service_names: Vec<&String> = service_sizes.keys().collect();
    sorted_service_names.sort();
    for service_name in sorted_service_names {
        if let Some(size) = service_sizes.get(service_name) {
            size_report_lines.push(format!("{}: {}", service_name, human_bytes(*size as f64)));
        }
    }
    size_report_lines.push("".to_string());
    size_report_lines.push(format!(
        "Total used size across all services (calculated from listings): {}",
        human_bytes(grand_total_size as f64)
    ));
    let size_report_string = size_report_lines.join("\n");
    fs::write(&size_output_path, &size_report_string)?;
    println!("Size report written to '{}'", size_output_path.display());

    // --- 5. Generate Tree/File Structure Report (Uses size_cache) ---
    match output_division_mode {
        OutputMode::Single => {
            let tree_report_string = files_data.generate_full_tree_string(
                &size_cache, // Pass cache
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
        OutputMode::Remotes => {
            let mut sorted_services: Vec<String> = files_data.get_service_names();
            sorted_services.sort();
            for service_name in sorted_services {
                let (service_string, _service_calculated_size) = files_data
                    .generate_service_tree_string(
                        &service_name,
                        &size_cache,
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                        remote_icon,
                    );
                let service_filename =
                    format!("{MODE_REMOTES_SERVICE_PREFIX}{service_name} {tree_base_filename}.txt");
                let service_output_path = output_dir.join(service_filename);
                fs::write(&service_output_path, service_string)?;
                println!(
                    "  - Remote file list written for '{}' to '{}'",
                    service_name,
                    service_output_path.display()
                );
            }
            println!(
                "Tree report (per remote) written to directory '{}'",
                output_dir.display()
            );
        }
        OutputMode::Folders => {
            let mut sorted_services: Vec<String> = files_data.get_service_names();
            sorted_services.sort();
            println!("Writing folder structure to '{}'...", output_dir.display());
            for service_name in sorted_services {
                let service_dir_path = output_dir.join(&service_name);
                let root_keys = files_data
                    .roots_by_service
                    .get(&service_name)
                    .cloned()
                    .unwrap_or_default();
                if root_keys.is_empty() {
                    println!("  - No root items found for remote '{}', skipping folder structure generation.", service_name);
                    continue;
                }

                // Need to sort roots here too: Dirs first, then alpha
                let mut sorted_root_keys = root_keys;
                sorted_root_keys.sort_by(|a_key, b_key| {
                    match (files_data.files.get(a_key), files_data.files.get(b_key)) {
                        (Some(a), Some(b)) => a
                            .is_dir
                            .cmp(&b.is_dir)
                            .reverse()
                            .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                        _ => Ordering::Equal, // Simplified error handling
                    }
                });
                println!(
                    "  - Processing remote '{}' (output root: '{}')...",
                    service_name,
                    service_dir_path.display()
                );
                for root_key in &sorted_root_keys {
                    if let Some(root_file) = files_data.files.get(root_key) {
                        root_file.write_fs_node_recursive(
                            &service_dir_path,
                            &files_data.files,
                            &size_cache,
                            folder_icon,
                            file_icon,
                            size_icon,
                            date_icon,
                            folder_content_filename,
                        )?;
                    } else {
                        eprintln!("Warning: Root key '{}' not found during Folder mode processing for service '{}'.", root_key, service_name);
                    }
                }
            }
            println!(
                "Tree report (folder structure) generation complete in directory '{}'",
                output_dir.display()
            );
        }
    }

    println!("Reports generation finished.");
    Ok(())
}

/// Processes the results from parallel `rclone about` calls.
/// Aggregates totals, formats the report, and writes it to disk.
pub fn process_about_results(
    results: Vec<AboutResult>, // Takes the collected results
    about_output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if results.is_empty() {
        println!("No 'about' results to process.");
        // Write an empty or placeholder report if desired, or just return
        fs::write(about_output_path, "No remote information available.")?;
        return Ok(());
    }
    println!("Processing 'about' results...");

    let mut report_lines: Vec<(String, String)> = Vec::new(); // (name, formatted_line) for sorting
    let mut grand_total_used: u64 = 0;
    let mut grand_total_free: u64 = 0;
    let mut grand_total_trashed: u64 = 0;
    let mut remotes_with_data = 0;
    let mut errors_encountered = 0;

    // Process results sequentially
    for result in results {
        match result {
            Ok((remote_name, about_info)) => {
                remotes_with_data += 1;

                // Accumulate totals
                grand_total_used += about_info.used.unwrap_or(0);
                grand_total_free += about_info.free.unwrap_or(0);
                grand_total_trashed += about_info.trashed.unwrap_or(0);

                // Format line
                let used_str = about_info
                    .used
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                let total_str = about_info
                    .total
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                let free_str = about_info
                    .free
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                let trashed_str = about_info.trashed.filter(|&v| v > 0).map_or_else(
                    || "".to_string(),
                    |v| format!(", Trashed={}", human_bytes(v as f64)),
                );
                let other_str = about_info.other.filter(|&v| v > 0).map_or_else(
                    || "".to_string(),
                    |v| format!(", Other={}", human_bytes(v as f64)),
                );

                let line = format!(
                    "{}: Used={}, Free={}, Total={}{}{}",
                    remote_name, used_str, free_str, total_str, trashed_str, other_str
                );
                report_lines.push((remote_name.clone(), line));
            }
            Err((remote_name, err_msg)) => {
                errors_encountered += 1;
                let line = format!("{}: Error - {}", remote_name, err_msg);
                // Optionally log the error here too, though it might have been logged during parallel exec
                eprintln!(
                    "  Error processing 'about' for {}: {}",
                    remote_name, err_msg
                );
                report_lines.push((remote_name.clone(), line));
            }
        }
    }

    // Sort lines alphabetically by remote name
    report_lines.sort_by(|a, b| a.0.cmp(&b.0));

    // Prepare final output string
    let mut final_report_lines: Vec<String> = Vec::new();
    final_report_lines.extend(report_lines.into_iter().map(|(_, line)| line));

    // Add Grand Total line
    if remotes_with_data > 0 {
        final_report_lines.push("".to_string());
        let effective_total_capacity = grand_total_used + grand_total_free;
        let grand_total_line = format!(
            "Grand Total ({} remotes reporting data): Used={}, Free={}, Trashed={}, Total Capacity={}",
            remotes_with_data,
            human_bytes(grand_total_used as f64),
            human_bytes(grand_total_free as f64),
            human_bytes(grand_total_trashed as f64),
            if effective_total_capacity > 0 { human_bytes(effective_total_capacity as f64) } else { "N/A".to_string() },
        );
        final_report_lines.push(grand_total_line);
    } else if errors_encountered > 0 {
        final_report_lines.push("".to_string());
        final_report_lines.push("Grand Total: No size data available due to errors.".to_string());
    } else {
        final_report_lines.push("".to_string());
        final_report_lines.push("Grand Total: No size data available from any remote.".to_string());
    }

    if errors_encountered > 0 {
        final_report_lines.push(format!(
            "\nNote: Encountered errors fetching 'about' info for {} remote(s).",
            errors_encountered
        ));
    }

    let final_report_string = final_report_lines.join("\n");
    fs::write(about_output_path, final_report_string)?;
    println!("About report written to '{}'", about_output_path);

    if errors_encountered > 0 {
        Ok(()) // FIXME Or return an error? return Ok for now, errors are logged
    } else {
        Ok(())
    }
}
