//! Core library for CloudMapper, providing data structures, rclone interaction logic,
//! and report generation functionalities for analyzing cloud storage.

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

/// Prefix used for filenames when outputting in `Remotes` mode.
const MODE_REMOTES_SERVICE_PREFIX: &str = "_";
/// Key used in the extensions report for files without a recognized extension.
const NO_EXTENSION_KEY: &str = "[no extension]";

/// Specifies how the output tree/file structure report should be generated.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "kebab-case")]
pub enum OutputMode {
    /// Output a single text file containing the tree structure for all remotes.
    Single,
    /// Output one text file per remote, each containing the tree structure for that remote.
    Remotes,
    /// Output a directory structure mirroring the remotes, with files containing folder contents.
    Folders,
}

// --- Data Structures ---

/// Represents the collection of hash sums for a file, as reported by `rclone lsjson --hash`.
#[derive(Serialize, Deserialize, Eq, Debug, Hash, Clone, Ord, PartialOrd, PartialEq)]
pub struct Hashes {
    /// SHA-1 hash, if available.
    #[serde(rename = "SHA-1", alias = "sha1")]
    sha1: Option<String>,
    /// Dropbox hash, if available.
    #[serde(rename = "DropboxHash", alias = "dropbox")]
    dropbox: Option<String>,
    /// MD5 hash, if available.
    #[serde(rename = "MD5", alias = "md5")]
    md5: Option<String>,
    /// SHA-256 hash, if available.
    #[serde(rename = "SHA-256", alias = "sha256")]
    sha256: Option<String>,
    /// QuickXorHash, if available (primarily OneDrive).
    #[serde(rename = "QuickXorHash", alias = "quickxor")]
    quickxor: Option<String>,
}

/// Represents a single file or directory entry as directly parsed from `rclone lsjson` output.
#[derive(Serialize, Deserialize, Eq, Debug, Clone)]
pub struct RawFile {
    /// The full path relative to the remote's root.
    #[serde(rename = "Path")]
    pub path: String,
    /// The base name of the file or directory.
    #[serde(rename = "Name")]
    pub name: String,
    /// Size in bytes. Can be -1 for directories or unknown sizes.
    #[serde(rename = "Size")]
    pub size: i64,
    /// MIME type, if available.
    #[serde(rename = "MimeType")]
    pub mime_type: String,
    /// Modification time in RFC3339 format (e.g., "2023-01-15T10:30:00Z").
    #[serde(rename = "ModTime")]
    pub mod_time: String,
    /// `true` if this entry is a directory, `false` otherwise.
    #[serde(rename = "IsDir")]
    pub is_dir: bool,
    /// Hash sums for the file, if available and requested. `None` for directories.
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
    /// Custom ordering for `RawFile`.
    /// Sorts directories before files.
    /// Then sorts by path depth (shallower first).
    /// Finally sorts alphabetically by full path.
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

/// Structure for deserializing the JSON output of `rclone about --json`.
/// Represents storage usage information for a remote.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")] 
pub struct RcloneAboutInfo {
    /// Total size of the remote storage, if available (bytes).
    total: Option<u64>,
    /// Used size on the remote storage, if available (bytes).
    used: Option<u64>,
    /// Size of data in the trash, if available (bytes).
    trashed: Option<u64>,
    /// Size of other data (e.g., system data), if available (bytes).
    other: Option<u64>,
    /// Free space available on the remote storage, if available (bytes).
    free: Option<u64>,
}

/// Helper type alias for the result of processing `rclone about` for a single remote in parallel.
/// Contains either the remote name and its `RcloneAboutInfo`, or the remote name and an error message.
pub type AboutResult = Result<(String, RcloneAboutInfo), (String, String)>;

/// Extracts the file stem (name without extension) and the lowercase extension from a path string.
///
/// # Arguments
/// * `path` - The full path or filename string.
///
/// # Returns
/// A tuple containing `(name, Option<extension>)`.
/// Extension is returned in lowercase. Returns `None` for extension if none is found.
/// If the path has no file stem (e.g., ".bashrc"), the original path is returned as the name.
fn get_name_and_extension(path: &str) -> (String, Option<String>) {
    let p = Path::new(path);
    let name = p
        .file_stem()
        .map_or_else(|| path.to_string(), |s| s.to_string_lossy().into_owned());
    // Get extension and convert to lowercase for consistent grouping
    let ext = p.extension().map(|s| s.to_string_lossy().to_lowercase());
    (name, ext)
}

/// Represents a processed file or directory within the internal tree structure.
#[derive(Debug, Clone)]
pub struct File {
    /// The name of the rclone remote this file belongs to.
    service: String,
    /// The file extension (lowercase), or empty string if no extension or it's a directory.
    /// Determined from `RawFile.name`.
    pub ext: String, 
    /// Path components relative to the service root (e.g., ["folder1", "subfolder", "file.txt"]).
    /// Derived directly from `RawFile.path`.
    path: Vec<String>,
    /// Modification time string (from `RawFile`).
    modified: String,
    /// Size in bytes (from `RawFile`).
    size: i64, 
    /// `true` if this is a directory, `false` otherwise.
    pub is_dir: bool, 
    /// Set of keys (`get_key()`) of the direct children of this directory. Empty for files.
    children_keys: HashSet<String>,
    /// Hash sums, if available. `None` for directories.
    hashes: Option<Hashes>,
}

/// Sanitizes a string to be safe for use as a filename or directory name on common filesystems,
/// particularly Windows. Replaces problematic characters with underscores.
/// Handles reserved characters, control characters, names ending with space/period,
/// and basic reserved OS names.
fn sanitize_for_filesystem(name: &str) -> String {
    // Replace characters that are invalid in Windows filenames/directory names.
    // Also replace control characters.
    let mut sanitized: String = name
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_', // Reserved characters
            '\x00'..='\x1F' => '_',                                      // ASCII Control characters
            _ => c,
        })
        .collect();

    // Windows filenames/directory names cannot end with a space or a period.
    // Replace the trailing character if it's a space or period.
    if let Some(last_char) = sanitized.chars().last() {
        if last_char == '.' || last_char == ' ' {
            sanitized.pop(); // Remove the problematic character
            sanitized.push('_'); // Append an underscore instead
        }
    }

    // If the name becomes empty after sanitization (e.g., was "???"), provide a placeholder.
    if sanitized.is_empty() {
        return "_empty_name_".to_string();
    }

    // Check against a list of reserved names on Windows (case-insensitive).
    // Prepend an underscore if it matches a reserved name.
    let lower_sanitized = sanitized.to_lowercase();
    let reserved_names = [
        "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
        "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
    ];

    if reserved_names.contains(&lower_sanitized.as_str()) {
        format!("_{}", sanitized)
    } else {
        sanitized
    }
}

impl File {
    /// Creates a `File` instance from a `RawFile` and the service name.
    /// Parses the path and extracts the extension from `RawFile.name`.
    fn from_raw(service: String, raw_file: &RawFile) -> Self {
        // Path components are taken directly from RawFile.path
        let path_components: Vec<String> = raw_file
            .path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        // Extension is determined from RawFile.name
        let (_name_stem, ext_option) = get_name_and_extension(&raw_file.name);

        Self {
            service,
            ext: ext_option.unwrap_or_default(), // Use empty string if no extension
            path: path_components,              // Contains components like ["dir1", "subdir", "file.txt"]
            modified: raw_file.mod_time.clone(),
            size: raw_file.size,
            is_dir: raw_file.is_dir,
            children_keys: HashSet::new(),
            hashes: raw_file.hashes.clone(),
        }
    }

    /// Generates a unique key for this file/directory node within the `Files` map.
    /// The key incorporates the service name and path. For files, it also includes
    /// extension and size to help disambiguate potential collisions.
    fn get_key(&self) -> String {
        // Key needs to uniquely identify this node, considering service and path.
        // For directories, path is enough. For files, add name+ext+size to disambiguate
        // files with the same name but potentially different content/metadata at the same path level
        // (though less common with rclone lsjson output structure).
        if self.is_dir {
            // For directories, the key is service + "/" + path components joined by "/"
            // Example: "remote1/folder/subfolder"
        format!("{}{}", self.service, self.path.join("/"))
        } else {
            // For files, the key includes service, path, extension, and size for better uniqueness
            // Example: "remote1/folder/filetxt-1024"
            format!(
                "{}{}{}{}",
                self.service,
                self.path.join("/"), // This joins path components like ["folder", "file"] -> "folder/file"
                self.ext,            // Append the extension (e.g., "txt")
                self.size            // Append the size (e.g., "-1024")
            )
        }
    }

    /// Generates the key for the potential parent directory of this file/directory.
    /// Returns `None` if the item is at the root of the service.
    fn get_parent_key(&self) -> Option<String> {
        if self.path.len() > 1 {
            let parent_path_vec = &self.path[..self.path.len() - 1];
            // Parent key must represent a directory, so use the directory key format: service + path
            Some(format!("{}{}", self.service, parent_path_vec.join("/")))
        } else {
            None // Root level file/dir in the service
        }
    }

    /// Gets the display name for the file or directory (the last path component).
    /// Returns the service name if the path is somehow empty (should not normally happen).
    fn get_display_name(&self) -> String {
        self.path
            .last()
            .cloned()
            .unwrap_or_else(|| self.service.clone()) // Fallback to service name if path is empty
    }

    /// Recursively formats a string representation for this node and its children,
    /// suitable for the tree view report. Uses the pre-calculated size cache.
    ///
    /// # Arguments
    /// * `indent_size` - Number of spaces per indentation level.
    /// * `all_files` - A map containing all `File` objects, keyed by `get_key()`.
    /// * `size_cache` - A map containing pre-calculated total sizes for each node key.
    /// * `folder_icon`, `file_icon`, `size_icon`, `date_icon` - Icons for formatting.
    ///
    /// # Returns
    /// A formatted string including the current node and all its descendants.
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
        let modified_str = &self.modified; // Use the stored modification time string

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
                // Log if a child key listed in children_keys is not found in the main map
                eprintln!("Warning: Child key '{}' not found in file map.", child_key);
            }
        }

        // Join the formatted lines of children
        let children_str = children_output.join("\n");
        let line_sep = if children_output.is_empty() { "" } else { "\n" };

        // Format the current node's entry line
        let entry_str = format!(
            "{}{} {} {size_icon} {} {date_icon} {}",
            indent, starter, name, size_str, modified_str
        );

        // Combine the current node's line with its children's lines
        format!("{}{}{}", entry_str, line_sep, children_str)
    }

    /// Formats a list of direct children for the `Folders` output mode.
    /// Uses the pre-calculated size cache.
    ///
    /// # Arguments
    /// * `all_files` - A map containing all `File` objects.
    /// * `size_cache` - A map containing pre-calculated total sizes for each node key.
    /// * `folder_icon`, `file_icon`, `size_icon`, `date_icon` - Icons for formatting.
    ///
    /// # Returns
    /// A string containing formatted lines for each direct child, ready to be written
    /// to a folder content file (e.g., `files.txt`).
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
                    // .reverse() // Directories first
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

                // Format line with slight indent for children list
                let line = format!(
                    "  {} {} {size_icon} {} {date_icon} {}",
                    starter, name, size_str, modified_str
                );
                children_lines.push(line);
            }
        }
        children_lines.join("\n")
    }

    /// Recursively writes the directory structure and content files for the `Folders` output mode.
    /// Creates directories on the filesystem and writes a content file (e.g., `files.txt`)
    /// inside each directory listing its children. Uses the pre-calculated size cache.
    ///
    /// Note: Files at the root of a service are not handled by this recursive function directly;
    /// they are typically listed in a summary file for the service's root, created by the caller.
    ///
    /// # Arguments
    /// * `parent_fs_path` - The filesystem path of the parent directory where this node (if a directory) should be created.
    /// * `all_files` - A map containing all `File` objects.
    /// * `size_cache` - A map containing pre-calculated total sizes for each node key.
    /// * `folder_icon`, `file_icon`, `size_icon`, `date_icon` - Icons for formatting content files.
    /// * `folder_content_filename` - The name of the file to write inside each directory (e.g., "files.txt").
    ///
    /// # Returns
    /// `Ok(())` on success, or an `io::Error` if filesystem operations fail.
    fn write_fs_node_recursive(
        &self,
        parent_fs_path: &Path,
        all_files: &HashMap<String, File>,
        size_cache: &HashMap<String, u64>, 
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        folder_content_filename: &str,
    ) -> io::Result<()> {
        // Only process directories in this function for recursive creation
        if !self.is_dir {
            return Ok(());
        }

        // Sanitize the display name for use as a filesystem path component.
        let display_name = self.get_display_name();
        let sanitized_display_name = sanitize_for_filesystem(&display_name);

        // Construct the path for the current directory on the filesystem
        let current_fs_path = parent_fs_path.join(&sanitized_display_name);
        // Ensure this directory exists. Log specific path on error.
        if let Err(e) = fs::create_dir_all(&current_fs_path) {
            eprintln!(
                "Error: Failed to create directory '{}' (original name: '{}'). OS error: {}",
                current_fs_path.display(),
                display_name, 
                e
            );
            return Err(e); 
        }
        // Generate the content list string using the helper function and cache
        let contents_string = self.format_direct_children_list(
            all_files,
            size_cache,
            folder_icon,
            file_icon,
            size_icon,
            date_icon,
        );

        let contents_file_path = current_fs_path.join(folder_content_filename);
        if let Err(e) = fs::write(&contents_file_path, contents_string) {
            eprintln!(
                "Error: Failed to write contents to '{}'. OS error: {}",
                contents_file_path.display(),
                e
            );
            return Err(e); 
        }

        // Sort children for deterministic processing order
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by_cached_key(|key| {
            all_files.get(*key).map_or_else(
                || String::new(), 
                |f| f.get_display_name(),
            )
        });

        // Recursively call this function for child DIRECTORIES only
        for child_key in sorted_children_keys {
            if let Some(child_file) = all_files.get(child_key) {
                if child_file.is_dir {
                    // Recurse into subdirectory
                    child_file.write_fs_node_recursive(
                        current_fs_path.as_path(), // New parent path is the current directory
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

/// Internal struct holding information about a set of duplicate files.
#[derive(Debug)]
struct DuplicateInfo {
    /// List of full paths (service:path/to/file) for files sharing the same hash.
    paths: Vec<String>,
    /// The size of each duplicate file in the set (bytes).
    size: u64,
}

/// Internal helper struct for aggregating data in the extensions report.
#[derive(Debug, Clone)]
struct ExtensionInfo {
    /// The file extension (lowercase, or `NO_EXTENSION_KEY`).
    extension: String,
    /// The number of files with this extension.
    count: u64,
    /// The total size of all files with this extension (bytes).
    total_size: u64,
}

// --- ECharts Data Structures ---

/// Represents a node in the data structure used for generating the ECharts treemap visualization.
#[derive(Serialize, Debug)]
struct EChartsTreemapNode {
    /// Display name of the node (file, directory, or service).
    name: String,
    /// Value of the node, typically the size in bytes. Used by treemap for area calculation.
    value: u64,
    /// Optional nested list of child nodes for directories or services. `None` for files.
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<Vec<EChartsTreemapNode>>,
}

/// Main container struct holding all processed file/directory data and related information.
#[derive(Debug)]
pub struct Files {
    /// Map storing all processed `File` objects, keyed by `file.get_key()`.
    pub files: HashMap<String, File>,
    /// Map storing information about duplicate files, keyed by `Hashes`.
    /// Populated by `find_duplicates()`.
    duplicates: BTreeMap<Hashes, DuplicateInfo>, // BTreeMap for sorted output in duplicates report
    /// Map storing the root keys (`file.get_key()`) for each service.
    /// Key is the service name, Value is a Vec of root keys belonging to that service.
    pub roots_by_service: HashMap<String, Vec<String>>,
    /// Internal map used during processing to group file keys by their `Hashes`.
    /// Key is `Hashes`, Value is a Vec of file keys (`file.get_key()`) sharing those hashes.
    hash_map: HashMap<Hashes, Vec<String>>,
}

impl Files {
    /// Creates a new, empty `Files` collection.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            duplicates: BTreeMap::new(),
            roots_by_service: HashMap::new(),
            hash_map: HashMap::new(),
        }
    }

    /// Adds a batch of `RawFile` entries for a specific service to the collection.
    /// Converts `RawFile` objects to internal `File` objects and stores them.
    /// Populates the `hash_map` for later duplicate detection.
    /// Identifies root nodes for the service.
    ///
    /// # Arguments
    /// * `service_name` - The name of the rclone remote.
    /// * `raw_files` - A vector of `RawFile` objects parsed from `rclone lsjson` for this service.
    pub fn add_remote_files(&mut self, service_name: &str, raw_files: Vec<RawFile>) {
        println!("Adding files for service: {}", service_name);
        let mut service_keys_added_this_run = HashSet::new();
        let mut sorted_raw_files = raw_files;
        // Sort raw files first to process directories before their contents (important for structure)
        sorted_raw_files.sort(); // Uses the custom Ord impl for RawFile

        for raw_file in sorted_raw_files {
            // Skip entries that are clearly invalid or represent the root directory itself
            if raw_file.path.is_empty() && raw_file.name.is_empty() && raw_file.is_dir {
                continue; 
            }
            if raw_file.path.is_empty() {
                eprintln!("Warning: Skipping raw file with empty path: {:?}", raw_file);
                continue;
            }
            if !raw_file.is_dir && raw_file.name.is_empty() {
                eprintln!("Warning: Skipping raw file with empty name: {:?}", raw_file);
                continue;
            }

            let file = File::from_raw(service_name.to_string(), &raw_file);
            let key = file.get_key();

            if !file.is_dir {
                if let Some(hashes) = &file.hashes {
                    if file.size >= 0 {
                        self.hash_map
                            .entry(hashes.clone()) 
                            .or_default() 
                            .push(key.clone()); 
                    }
                }
            }

            if self.files.insert(key.clone(), file.clone()).is_some() {
                if let Some(existing_file) = self.files.get(&key) {
                    if existing_file.is_dir != file.is_dir {
                        eprintln!("Warning: Overwriting existing key '{}' with different type (is_dir: {} -> {}). Check rclone output consistency.", key, existing_file.is_dir, file.is_dir);
                    }
                }
                self.files.insert(key.clone(), file);
            }
            service_keys_added_this_run.insert(key);
        }

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

    /// Builds the parent-child relationships between `File` objects.
    /// Iterates through all files, determines their parent key, and updates the
    /// parent's `children_keys` set. Should be called after all files from all
    /// services have been added via `add_remote_files`.
    pub fn build_tree(&mut self) {
        println!("Building tree structure...");
        let mut parent_child_map: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: Collect all parent-child relationships based on keys
        // Get a list of all keys currently in the main `files` map
        let all_keys: Vec<String> = self.files.keys().cloned().collect();

        for key in all_keys {
            if let Some(file) = self.files.get(&key) {
                if let Some(parent_key) = file.get_parent_key() {
                    if let Some(parent_file) = self.files.get(&parent_key) {
                        if parent_file.is_dir {
                            parent_child_map
                                .entry(parent_key.clone()) 
                                .or_default() 
                                .insert(key.clone()); 
                        } else {
                            eprintln!(
                                "Warning: Potential parent key '{}' for child '{}' exists but is not marked as a directory. Cannot assign child.",
                                parent_key, key
                            );
                        }
                    } else {
                        eprintln!(
                            "Warning: Parent key '{}' not found for file '{}'. File might be orphaned in tree view.",
                            parent_key, key
                        );
                    }
                }
            }
        }

        // Second pass: Update the actual `children_keys` field in the parent File objects
        for (parent_key, children_keys) in parent_child_map {
            if let Some(parent_file) = self.files.get_mut(&parent_key) {
                parent_file.children_keys = children_keys;
            }
        }
        println!("Tree structure built.");
    }

    /// Identifies duplicate files based on the `hash_map` populated during `add_remote_files`.
    /// Populates the `duplicates` map with `DuplicateInfo` for sets of files sharing the same hash.
    /// Only considers files with valid hashes and size >= 0.
    pub fn find_duplicates(&mut self) {
        println!("Finding duplicates based on hashes...");
        self.duplicates.clear(); 
        for (hash, keys) in &self.hash_map {
            if keys.len() > 1 {
                let mut paths = Vec::new(); 
                let mut size: Option<u64> = None; 
                let mut valid_files_found = 0; 

                for key in keys {
                    if let Some(file) = self.files.get(key) {
                        if !file.is_dir && file.size >= 0 {
                            let service_and_path =
                                format!("{}:{}", file.service, file.path.join("/"));
                            paths.push(service_and_path);
                            valid_files_found += 1;

                            if size.is_none() {
                                size = Some(file.size as u64);
                            } else if size != Some(file.size as u64) {
                                // Current behavior: Uses the size of the first valid file found for the set 
                                // and logs a warning if subsequent files with the same hash have different sizes.
                                eprintln!(
                                    "Warning: Files with same hash {:?} have different sizes reported: {} vs {} (Key: {})",
                                    hash,
                                    human_bytes(size.unwrap() as f64), 
                                    human_bytes(file.size as f64), 
                                    key
                                );
                            }
                        }
                    }
                }

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

    /// Calculates the total size for each node (file or directory) recursively.
    /// Uses a cache (`RefCell<HashMap<String, u64>>`) for efficiency.
    ///
    /// # Returns
    /// A tuple containing:
    /// * `grand_total_size`: Total calculated size across all services (bytes).
    /// * `service_sizes`: A map of service name to its total calculated size (bytes).
    /// * `size_cache`: The populated cache map (node key -> calculated size in bytes).
    fn calculate_all_sizes_with_cache(&self) -> (u64, HashMap<String, u64>, HashMap<String, u64>) {
        let mut grand_total_size: u64 = 0;
        let mut service_sizes: HashMap<String, u64> = HashMap::new();
        let size_cache = RefCell::new(HashMap::new());

        fn calculate_recursive_and_cache(
            key: &str,                             
            all_files: &HashMap<String, File>,     
            cache: &RefCell<HashMap<String, u64>>, 
        ) -> u64 {
            if let Some(&cached_size) = cache.borrow().get(key) {
                return cached_size;
            }

            let calculated_size = if let Some(file) = all_files.get(key) {
                if !file.is_dir {
                    if file.size >= 0 {
                        file.size as u64
                    } else {
                        0 
                    }
                } else {
                    let mut total_size: u64 = 0;
                    for child_key in &file.children_keys {
                        total_size += calculate_recursive_and_cache(child_key, all_files, cache);
                    }
                    total_size 
                }
            } else {
                eprintln!("Warning: Key '{}' not found during size calculation.", key);
                0 
            };

            cache.borrow_mut().insert(key.to_string(), calculated_size);
            calculated_size
        }

        for service_name in self.roots_by_service.keys() {
            let mut service_total_size: u64 = 0;
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned() 
                .unwrap_or_default(); 

            for root_key in &root_keys {
                service_total_size +=
                    calculate_recursive_and_cache(root_key, &self.files, &size_cache);
            }
            grand_total_size += service_total_size;
            service_sizes.insert(service_name.clone(), service_total_size);
        }

        (grand_total_size, service_sizes, size_cache.into_inner())
    }

    /// Generates a single formatted string representing the tree structure for all services combined.
    /// Uses the pre-calculated size cache. Suitable for `OutputMode::Single`.
    ///
    /// # Arguments
    /// * `size_cache` - Populated cache map (node key -> calculated size).
    /// * `folder_icon`, `file_icon`, `size_icon`, `date_icon`, `remote_icon` - Icons for formatting.
    /// * `service_sizes` - Map of service name -> total calculated size.
    ///
    /// # Returns
    /// A single string containing the formatted tree for all remotes.
    fn generate_full_tree_string(
        &self,
        size_cache: &HashMap<String, u64>, 
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
        service_sizes: &HashMap<String, u64>, 
    ) -> String {
        let mut final_text: Vec<String> = Vec::new();

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

    /// Generates the formatted tree string for a single specified service.
    /// Uses the pre-calculated size cache. Suitable for `OutputMode::Remotes`.
    ///
    /// # Arguments
    /// * `service_name` - The name of the service to generate the report for.
    /// * `size_cache` - Populated cache map (node key -> calculated size).
    /// * `folder_icon`, `file_icon`, `size_icon`, `date_icon`, `remote_icon` - Icons for formatting.
    ///
    /// # Returns
    /// A tuple containing:
    /// * The formatted tree string for the specified service.
    /// * The total calculated size for that service (retrieved from cache).
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
        let mut service_total_size: u64 = 0; // Initialize total size for this service
        let mut service_entries_lines: Vec<String> = Vec::new();

        let root_keys = self
            .roots_by_service
            .get(service_name)
            .cloned()
            .unwrap_or_default();

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
            human_bytes(service_total_size as f64) 
        );
        let body = service_entries_lines.join("\n");
        (format!("{}\n{}", header, body), service_total_size)
    }

    /// Generates the formatted text report for duplicate files.
    /// Uses parallel sorting (`rayon`) to order duplicate sets by potential space saving.
    ///
    /// # Returns
    /// A string containing the formatted duplicates report.
    pub fn generate_duplicates_output(&self) -> String {
        if self.duplicates.is_empty() {
            return "No duplicate files found based on available hashes.".to_string();
        }

        let mut sorted_duplicates: Vec<(&Hashes, &DuplicateInfo)> =
            self.duplicates.iter().collect();

        // Use rayon's parallel sort for potentially large lists of duplicates.
        // Sorts unstable (doesn't preserve order of equal elements, but faster).
        // Sort primarily by potential wasted space (descending), then by size (desc), then by count (desc).
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

            for path_key in &info.paths {
                lines.push(format!("  - {}", path_key)); 
            }

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

    /// Generates the formatted text report summarizing file counts and total sizes per extension.
    /// Uses parallel sorting (`rayon`) to order extensions primarily by total size (descending).
    ///
    /// # Returns
    /// A string containing the formatted extensions report.
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

        let mut sorted_extensions: Vec<ExtensionInfo> = extensions_map
            .into_iter()
            .map(|(ext, (count, size))| ExtensionInfo {
                extension: ext,
                count,
                total_size: size,
            })
            .collect();

        sorted_extensions.par_sort_unstable_by(|a, b| {
            b.total_size 
                .cmp(&a.total_size)
                .then_with(|| b.count.cmp(&a.count)) 
                .then_with(|| a.extension.cmp(&b.extension)) 
        });

        let mut lines: Vec<String> = Vec::new();
        lines.push(format!(
            "Total Files Found: {} (Total Size: {})",
            total_file_count,
            human_bytes(total_files_size as f64)
        ));
        lines.push("-------------------------------------".to_string());
        lines.push(format!(
            "{:<15} | {:>15} | {:>12} | {:>8} | {:>8}",
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

            lines.push(format!(
                "{:<15} | {:>15} | {:>12} | {:>7.2}% | {:>7.2}%", 
                ext_display,                                      
                human_bytes(info.total_size as f64),              
                info.count,                                       
                size_percent,                                     
                count_percent                                     
            ));
        }
        lines.join("\n")
    }

    /// Generates the formatted text report for the N largest files.
    /// Uses parallel sorting (`rayon`) to order files by size (descending).
    ///
    /// # Arguments
    /// * `num_largest_files` - The number of largest files to include in the report.
    ///
    /// # Returns
    /// A string containing the formatted largest files report.
    pub fn generate_largest_files_report(&self, num_largest_files: usize) -> String {
        if num_largest_files == 0 { // FIXME for <= 0
            return "Largest files report disabled (count is 0).".to_string();
        }

        println!("Identifying top {} largest files...", num_largest_files);

        let mut all_actual_files: Vec<&File> = self
            .files
            .values()
            .filter(|f| !f.is_dir && f.size >= 0) 
            .collect();

        if all_actual_files.is_empty() {
            return "No files found to generate largest files report.".to_string();
        }

        all_actual_files.par_sort_unstable_by(|a, b| b.size.cmp(&a.size));

        let mut lines: Vec<String> = Vec::new();
        let actual_num_to_show = num_largest_files.min(all_actual_files.len());

        lines.push(format!(
            "Top {} Largest Files (across all remotes):",
            actual_num_to_show
        ));
        lines.push(
            "----------------------------------------------------------------------".to_string(),
        ); 
        lines.push(format!(
            "{:<5} | {:>15} | {}", 
            "Rank", "Size", "Path (Service:File)"
        ));
        lines.push(
            "----------------------------------------------------------------------".to_string(),
        ); 

        for (i, file) in all_actual_files.iter().take(actual_num_to_show).enumerate() {
            let rank = i + 1;
            let size_str = human_bytes(file.size as f64);
            let full_path = format!("{}:{}", file.service, file.path.join("/"));

            lines.push(format!(
                "{:<5} | {:>15} | {}", 
                rank, size_str, full_path
            ));
        }

        if all_actual_files.len() > actual_num_to_show {
            lines.push("".to_string());
            lines.push(format!(
                "... and {} more files not listed.",
                all_actual_files.len() - actual_num_to_show
            ));
        }

        lines.join("\n")
    }

    /// Gets a sorted list of unique service names present in the collection.
    ///
    /// # Returns
    /// A `Vec<String>` containing the sorted names of the rclone remotes processed.
    pub fn get_service_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.roots_by_service.keys().cloned().collect();
        names.sort(); 
        names
    }

    /// Generates hierarchical data suitable for ECharts treemap visualization.
    /// Uses the pre-calculated size cache.
    ///
    /// # Arguments
    /// * `size_cache` - Populated cache map (node key -> calculated size).
    /// * `service_sizes` - Map of service name -> total calculated size.
    ///
    /// # Returns
    /// A `Vec<EChartsTreemapNode>` representing the root nodes (services)
    /// and their nested children, ready for JSON serialization.
    fn generate_echarts_data(
        &self,
        size_cache: &HashMap<String, u64>, 
        service_sizes: &HashMap<String, u64>, 
    ) -> Vec<EChartsTreemapNode> {
        println!("Generating data for ECharts treemap...");
        let mut treemap_data: Vec<EChartsTreemapNode> = Vec::new();

        fn build_echarts_node_recursive(
            key: &str,                         
            all_files: &HashMap<String, File>, 
            size_cache: &HashMap<String, u64>, 
        ) -> Option<EChartsTreemapNode> {
            if let Some(file) = all_files.get(key) {
                let name = file.get_display_name(); 
                let size = size_cache.get(key).cloned().unwrap_or(0);

                if !file.is_dir {
                    if size > 0 {
                        Some(EChartsTreemapNode {
                            name,
                            value: size,    
                            children: None, 
                        })
                    } else {
                        None 
                    }
                } else {
                    let mut children_nodes: Vec<EChartsTreemapNode> = Vec::new();
                    let mut sorted_children_keys: Vec<&String> =
                        file.children_keys.iter().collect();
                    sorted_children_keys.sort_by(|a_key, b_key| {
                        match (all_files.get(*a_key), all_files.get(*b_key)) {
                            (Some(a), Some(b)) => a
                                .is_dir
                                .cmp(&b.is_dir) 
                                .reverse() 
                                .then_with(|| a.get_display_name().cmp(&b.get_display_name())), 
                            _ => Ordering::Equal, 
                        }
                    });

                    for child_key in sorted_children_keys {
                        if let Some(child_node) =
                            build_echarts_node_recursive(child_key, all_files, size_cache)
                        {
                            children_nodes.push(child_node);
                        }
                    }

                    if size > 0 || !children_nodes.is_empty() {
                        Some(EChartsTreemapNode {
                            name,
                            value: size, 
                            children: if children_nodes.is_empty() {
                                None
                            } else {
                                Some(children_nodes)
                            }, 
                        })
                    } else {
                        None 
                    }
                }
            } else {
                eprintln!(
                    "Warning: Key '{}' not found during ECharts data generation.",
                    key
                );
                None 
            }
        }

        // --- Main ECharts data generation loop ---
        let mut sorted_service_names: Vec<&String> = self.roots_by_service.keys().collect();
        sorted_service_names.sort();

        for service_name in sorted_service_names {
            let mut service_children: Vec<EChartsTreemapNode> = Vec::new();
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            let mut sorted_root_keys = root_keys;
            sorted_root_keys.sort_by(|a_key, b_key| {
                match (self.files.get(a_key), self.files.get(b_key)) {
                    (Some(a), Some(b)) => a
                        .is_dir
                        .cmp(&b.is_dir) 
                        .reverse() 
                        .then_with(|| a.get_display_name().cmp(&b.get_display_name())), 
                    _ => Ordering::Equal, 
                }
            });

            for root_key in &sorted_root_keys {
                if let Some(root_node) =
                    build_echarts_node_recursive(root_key, &self.files, size_cache)
                {
                    service_children.push(root_node);
                }
            }

            let service_total_size = service_sizes.get(service_name).cloned().unwrap_or(0);

            if service_total_size > 0 || !service_children.is_empty() {
                treemap_data.push(EChartsTreemapNode {
                    name: service_name.clone(), 
                    value: service_total_size,  
                    children: if service_children.is_empty() {
                        None
                    } else {
                        Some(service_children)
                    }, 
                });
            }
        }

        println!("ECharts data generation complete.");
        treemap_data 
    }
}

/// Generates the full HTML content for the ECharts treemap visualization.
/// Embeds the provided data and necessary JavaScript.
///
/// # Arguments
/// * `data` - A slice of `EChartsTreemapNode` representing the hierarchical data.
/// * `title` - The title to be used in the HTML `<title>` tag.
///
/// # Returns
/// A `Result` containing the HTML string, or a `serde_json::Error` if data serialization fails.
fn generate_echarts_html(
    data: &[EChartsTreemapNode],
    title: &str, 
) -> Result<String, serde_json::Error> {
    let data_json = serde_json::to_string(data)?;

    let colors_json = serde_json::to_string(&[
        "#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de", "#3ba272", "#fc8452", "#9a60b4",
        "#ea7ccc",
    ])?; 

    let specific_title =
        "Cloud Storage Treemap - Hover over items to see more info. Click to drill down.";

    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title> 
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script> 
    <style>
        html, body {{ height: 100%; margin: 0; padding: 0; overflow: hidden; }}
        #main {{ height: 100%; width: 100%; background-color: #000000; }} 
        .tooltip-title {{ font-weight: bold; margin-bottom: 5px; }} 
    </style>
</head>
<body>
    <div id="main"></div> 
    <script type="text/javascript">
        var chartDom = document.getElementById('main');
        var myChart = echarts.init(chartDom, 'dark'); 
        var option;

        var data = {data_json}; 
        var colors = {colors_json}; 

        function formatBytes(bytes, decimals = 2) {{
            if (!+bytes) return '0 Bytes' 
            const k = 1024 
            const dm = decimals < 0 ? 0 : decimals
            const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
            const i = Math.max(0, Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k))));
            return `${{parseFloat((bytes / Math.pow(k, i)).toFixed(dm))}} ${{sizes[i]}}`
        }}

        option = {{
            title: {{
                text: '{specific_title}', 
                left: 'center', 
                textStyle: {{ color: '#fff' }} 
            }},
            color: colors,
            tooltip: {{ 
                formatter: function (info) {{ 
                    var value = info.value; 
                    var treePathInfo = info.treePathInfo; 
                    var treePath = [];
                    for (var i = 1; i < treePathInfo.length; i++) {{
                        treePath.push(treePathInfo[i].name);
                    }}
                    return '<div class="tooltip-title">' + echarts.format.encodeHTML(treePath.join('/')) + '</div>' 
                         + 'Size: ' + formatBytes(value); 
                }}
            }},
            series: [
                {{
                    name: 'Cloud Storage', 
                    type: 'treemap', 
                    roam: true, 
                    nodeClick: 'zoomToNode', 
                    squareRatio: 0.8, 
                    colorSaturation: [0.5, 0.9], 
                    breadcrumb: {{ 
                        show: true, 
                        height: 22, 
                        left: 'center', 
                        bottom: '5%', 
                         itemStyle: {{ 
                            textStyle: {{
                                color: '#fff' 
                            }}
                         }},
                         emphasis: {{ 
                            itemStyle: {{
                                textStyle: {{ color: '#ddd' }} 
                            }}
                         }}
                    }},
                    label: {{ 
                        show: false, 
                        position: 'inside',
                        formatter: function (params) {{ 
                            return params.name + '\n' + formatBytes(params.value);
                         }},
                         color: '#fff', 
                         overflow: 'truncate', 
                    }},
                    upperLabel: {{ 
                        show: false, 
                        color: '#fff',
                    }},
                    itemStyle: {{ // Default style for deeper treemap nodes
                        borderColor: '#333', // Dark grey border, visible on black background
                        borderWidth: 0.5, 
                        gapWidth: 0.0,
                    }},
                    levels: [ // Define specific styles for different levels of the hierarchy
                        {{ // Level 0 (ECharts depth 1): Top-level services
                            itemStyle: {{ borderColor: '#ff6361', borderWidth: 15, gapWidth: 0, borderRadius: 15 }} // Subtle border, small gap
                        }},
                        {{ // Level 1 (ECharts depth 2): First level folders/files within a service
                            itemStyle: {{ borderColor: '#fff', borderWidth: 15, gapWidth: 0, borderRadius: 15 }} // Thinner border, smaller gap
                        }},
                    ],
                    data: data 
                }}
            ]
        }};

        myChart.setOption(option);
        window.addEventListener('resize', myChart.resize);

    </script>
</body>
</html>"#,
        title = title,                   
        specific_title = specific_title, 
        data_json = data_json,           
        colors_json = colors_json        
    ))
}

// --- Public Functions ---

/// Executes an external command (like `rclone`) and captures its output.
/// Handles basic error checking and logging.
///
/// # Arguments
/// * `executable` - The name or path of the command to run.
/// * `args` - A slice of string arguments to pass to the command.
///
/// # Returns
/// A `Result` containing the `std::process::Output` on success, or an `io::Error` on failure
/// (e.g., command not found, permission denied). Logs errors to stderr if the command runs
/// but returns a non-zero exit code.
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

    let output_result = Command::new(executable)
        .args(args) 
        .stdout(Stdio::piped()) 
        .stderr(Stdio::piped()) 
        .output(); 

    match output_result {
        Ok(ref out) => {
            if !out.status.success() {
                let stderr_str = String::from_utf8_lossy(&out.stderr); 
                if !stderr_str.is_empty() {
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
            Ok(out.clone()) 
        }
        Err(ref e) => {
            eprintln!("  Failed to execute command '{}': {}", executable, e);
            if e.kind() == ErrorKind::NotFound {
                eprintln!(
                    "  Hint: Make sure '{}' is installed and in your system's PATH, or provide the full path.",
                    executable
                );
            }
            Err(io::Error::new(e.kind(), e.to_string())) 
        }
    }
}

/// Parses the JSON output string from `rclone lsjson`.
///
/// # Arguments
/// * `json_data` - A string containing the JSON output from `rclone lsjson`.
///
/// # Returns
/// A `Result` containing a `Vec<RawFile>` on successful parsing,
/// or a `serde_json::Error` if the JSON is invalid.
pub fn parse_rclone_lsjson(json_data: &str) -> Result<Vec<RawFile>, serde_json::Error> {
    serde_json::from_str(json_data)
}

/// Orchestrates the generation of all requested reports based on the processed file data.
/// Builds the tree structure, finds duplicates, calculates sizes, and writes reports
/// to the specified output directory according to the chosen mode.
///
/// # Arguments
/// * `files_data` - Mutable reference to the `Files` struct containing all processed data.
/// * `output_division_mode` - The selected `OutputMode` (Single, Remotes, Folders).
/// * `enable_duplicates_report` - Flag to enable/disable duplicates report generation.
/// * `enable_extensions_report` - Flag to enable/disable extensions report generation.
/// * `num_largest_files` - Number of largest files to report (0 to disable).
/// * `enable_html_treemap` - Flag to enable/disable HTML treemap generation.
/// * `output_dir` - Path to the directory where reports will be saved.
/// * `tree_base_filename` - Base filename for tree/content files (e.g., "files.txt").
/// * `folder_content_filename` - Filename used inside directories in `Folders` mode.
/// * `duplicates_output_filename` - Filename for the duplicates report.
/// * `size_output_filename` - Filename for the size summary report.
/// * `extensions_output_filename` - Filename for the extensions report.
/// * `largest_files_output_filename` - Filename for the largest files report.
/// * `treemap_output_filename` - Filename for the HTML treemap report.
/// * `folder_icon`, `file_icon`, `size_icon`, `date_icon`, `remote_icon` - Icons for formatting text reports.
///
/// # Returns
/// `Ok(())` on success, or a `Box<dyn std::error::Error>` if any report generation or
/// filesystem operation fails.
#[allow(clippy::too_many_arguments)]
pub fn generate_reports(
    files_data: &mut Files,
    output_division_mode: OutputMode,
    enable_duplicates_report: bool,
    enable_extensions_report: bool,
    num_largest_files: usize, 
    enable_html_treemap: bool,
    output_dir: &Path,
    tree_base_filename: &str,
    folder_content_filename: &str,
    duplicates_output_filename: &str,
    size_output_filename: &str,
    extensions_output_filename: &str,
    largest_files_output_filename: &str, 
    treemap_output_filename: &str,
    folder_icon: &str,
    file_icon: &str,
    size_icon: &str,
    date_icon: &str,
    remote_icon: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating reports (Mode: {:?})...", output_division_mode);
    fs::create_dir_all(output_dir)?;

    files_data.build_tree();

    let duplicates_output_path = output_dir.join(duplicates_output_filename);
    if enable_duplicates_report {
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
        }
    }

    let extensions_output_path = output_dir.join(extensions_output_filename);
    if enable_extensions_report {
        let extensions_report_string = files_data.generate_extensions_report();
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

    let largest_files_output_path = output_dir.join(largest_files_output_filename);
    if num_largest_files > 0 {
        println!(
            "Generating largest files report (top {})...",
            num_largest_files
        );
        let largest_files_report_string =
            files_data.generate_largest_files_report(num_largest_files);
        fs::write(&largest_files_output_path, largest_files_report_string)?;
        println!(
            "Largest files report written to '{}'",
            largest_files_output_path.display()
        );
    } else {
        println!("Largest files report skipped (count is 0).");
        if largest_files_output_path.exists() {
            let _ = fs::remove_file(&largest_files_output_path);
        }
    }

    println!("Calculating sizes and populating cache...");
    let (grand_total_size, service_sizes, size_cache) = files_data.calculate_all_sizes_with_cache();
    println!("Size calculation complete.");

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

    match output_division_mode {
        OutputMode::Single => {
            let tree_report_string = files_data.generate_full_tree_string(
                &size_cache, 
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
                    format!("{MODE_REMOTES_SERVICE_PREFIX}{service_name} {tree_base_filename}"); 
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
                // Create the service base directory first.
                // The recursive call will create subdirectories within this.
                fs::create_dir_all(&service_dir_path)?;

                let root_keys = files_data
                    .roots_by_service
                    .get(&service_name)
                    .cloned()
                    .unwrap_or_default();

                if root_keys.is_empty() {
                    println!("  - No root items found for remote '{}', skipping folder structure generation.", service_name);
                    continue; 
                }

                let mut sorted_root_keys = root_keys;
                sorted_root_keys.sort_by(|a_key, b_key| {
                    match (files_data.files.get(a_key), files_data.files.get(b_key)) {
                        (Some(a), Some(b)) => a
                            .is_dir
                            .cmp(&b.is_dir) 
                            .reverse() 
                            .then_with(|| a.get_display_name().cmp(&b.get_display_name())), 
                        _ => Ordering::Equal, 
                    }
                });
                
                // For files directly under the service root, list them in a `folder_content_filename`
                // at the service_dir_path level.
                let mut root_level_content_lines = Vec::new();
                 println!(
                    "  - Processing remote '{}' (output root: '{}')...",
                    service_name,
                    service_dir_path.display()
                );
                for root_key in &sorted_root_keys {
                    if let Some(root_file) = files_data.files.get(root_key) {
                        if root_file.is_dir {
                            // For directories, start recursive writing. parent_fs_path is the service_dir_path itself
                            // as sanitize_for_filesystem expects the *parent* of the dir to be created.
                            // write_fs_node_recursive will create `service_dir_path/root_file_display_name`
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
                            // For files at the root, add them to the root-level content list.
                            let name = root_file.get_display_name();
                            let starter = file_icon;
                            let display_size = size_cache.get(root_key).cloned().unwrap_or(0);
                            let size_str = human_bytes(display_size as f64);
                            let modified_str = &root_file.modified;
                            let line = format!(
                                "  {} {} {size_icon} {} {date_icon} {}",
                                starter, name, size_str, modified_str
                            );
                            root_level_content_lines.push(line);
                        }
                    } else {
                        eprintln!("Warning: Root key '{}' not found during Folder mode processing for service '{}'.", root_key, service_name);
                    }
                }
                 // Write the content file for the service's root directory if there were root files.
                if !root_level_content_lines.is_empty() {
                    let root_content_file_path = service_dir_path.join(folder_content_filename);
                    let root_content_string = root_level_content_lines.join("\n");
                    // Add a header for the service itself in this root file.
                    let service_total_size = service_sizes.get(&service_name).cloned().unwrap_or(0);
                    let header = format!(
                        "{} {}: {}\n", // Add newline
                        remote_icon,
                        service_name,
                        human_bytes(service_total_size as f64)
                    );
                    let final_content = format!("{}{}", header, root_content_string);

                    if let Err(e) = fs::write(&root_content_file_path, final_content) {
                        eprintln!(
                            "Error: Failed to write root contents to '{}'. OS error: {}",
                            root_content_file_path.display(), e
                        );
                        // Potentially return Err(e.into()) here if this is critical
                    }
                }
            }
            println!(
                "Tree report (folder structure) generation complete in directory '{}'",
                output_dir.display()
            );
        }
    }

    let treemap_output_path = output_dir.join(treemap_output_filename);
    if enable_html_treemap {
        println!("Generating HTML treemap visualization...");
        match files_data.generate_echarts_data(&size_cache, &service_sizes) {
            echarts_data => {
                match generate_echarts_html(&echarts_data, "Cloud Storage Treemap") {
                    Ok(html_content) => {
                        if let Err(e) = fs::write(&treemap_output_path, html_content) {
                            eprintln!(
                                "Error writing HTML treemap report to '{}': {}",
                                treemap_output_path.display(),
                                e
                            );
                        } else {
                            println!(
                                "HTML treemap report written to '{}'",
                                treemap_output_path.display()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("Error generating ECharts HTML content: {}", e);
                    }
                }
            } 
        }
    } else {
        println!("HTML treemap visualization skipped.");
        if treemap_output_path.exists() {
            let _ = fs::remove_file(&treemap_output_path);
        }
    }

    println!("Reports generation finished.");
    Ok(()) 
}

/// Processes the results from parallel `rclone about` command executions.
/// Aggregates storage totals, formats a summary report, and writes it to disk.
///
/// # Arguments
/// * `results` - A vector containing the `AboutResult` from each parallel `rclone about` task.
/// * `about_output_path` - The path where the final 'about' report should be written.
///
/// # Returns
/// `Ok(())` on success, even if some individual remotes failed. Errors during processing or
/// writing the final report will return a `Box<dyn std::error::Error>`. Errors fetching
/// individual remote info are logged to stderr.
pub fn process_about_results(
    results: Vec<AboutResult>, 
    about_output_path: &str,   
) -> Result<(), Box<dyn std::error::Error>> {
    if results.is_empty() {
        println!("No 'about' results to process.");
        fs::write(about_output_path, "No remote information available.")?;
        return Ok(());
    }
    println!("Processing 'about' results...");

    let mut report_lines: Vec<(String, String)> = Vec::new();
    let mut grand_total_used: u64 = 0;
    let mut grand_total_free: u64 = 0;
    let mut grand_total_trashed: u64 = 0;
    let mut remotes_with_data = 0; 
    let mut errors_encountered = 0; 

    for result in results {
        match result {
            Ok((remote_name, about_info)) => {
                remotes_with_data += 1;

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
                eprintln!(
                    "  Error processing 'about' for {}: {}",
                    remote_name, err_msg
                );
                report_lines.push((remote_name.clone(), line)); 
            }
        }
    }

    report_lines.sort_by(|a, b| a.0.cmp(&b.0));

    let mut final_report_lines: Vec<String> = Vec::new();
    final_report_lines.extend(report_lines.into_iter().map(|(_, line)| line));

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
            "\nNote: Encountered errors fetching 'about' info for {} remote(s). See logs for details.",
            errors_encountered
        ));
    }

    let final_report_string = final_report_lines.join("\n");
    fs::write(about_output_path, final_report_string)?; 
    println!("About report written to '{}'", about_output_path);

    // Errors for individual remotes are logged; overall success is returned if file write succeeds.
    Ok(())
}