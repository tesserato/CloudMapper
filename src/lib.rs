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
#[serde(rename_all = "camelCase")] // Handles fields like "camelCase" if needed, though rclone uses lowercase
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
    pub ext: String, // Keep public for extension report access
    /// Path components relative to the service root (e.g., ["folder1", "subfolder", "file"]).
    path: Vec<String>,
    /// Modification time string (from `RawFile`).
    modified: String,
    /// Size in bytes (from `RawFile`).
    size: i64, // Keep as i64 to handle potential -1 from rclone
    /// `true` if this is a directory, `false` otherwise.
    pub is_dir: bool, // Keep public for reporting access
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
    /// Parses the path and extracts the extension.
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
        // This logic attempts to correct the path if the Name field was just the stem,
        // but the Path field included the extension (common rclone behavior).
        if !raw_file.is_dir && ext_opt.is_some() {
            if let Some(last) = final_path.last_mut() {
                // Basic check to avoid replacing a component like 'archive.tar.gz' with 'archive.tar'
                if !last.ends_with(&format!(".{}", ext_opt.as_deref().unwrap_or(""))) {
                    // Check if the last path component already matches the name_part from get_name_and_extension
                    if *last != name_part {
                        // If not, and it doesn't end with the extension, update it to the name_part.
                        // This handles cases where Name="file" and Path="dir/file.txt",
                        // ensuring the final path component becomes "file" to match the internal representation.
                        *last = name_part; // Apply if last component doesn't look like it includes the ext
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
    /// # Arguments
    /// * `parent_fs_path` - The filesystem path of the parent directory.
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
        size_cache: &HashMap<String, u64>, // Added size cache parameter
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        folder_content_filename: &str,
    ) -> io::Result<()> {
        // Only process directories in this function
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
                display_name, // Log original name for context
                e
            );
            return Err(e); // Propagate the error
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

        // Write the content list to the specified file within the current directory
        // folder_content_filename is assumed to be safe (e.g., "files.txt")
        let contents_file_path = current_fs_path.join(folder_content_filename);
        if let Err(e) = fs::write(&contents_file_path, contents_string) {
            eprintln!(
                "Error: Failed to write contents to '{}'. OS error: {}",
                contents_file_path.display(),
                e
            );
            return Err(e); // Propagate the error
        }

        // Sort children for deterministic processing order
        let mut sorted_children_keys: Vec<&String> = self.children_keys.iter().collect();
        sorted_children_keys.sort_by_cached_key(|key| {
            all_files.get(*key).map_or_else(
                || String::new(), // Should not happen if data is consistent
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
                // Files within this directory are handled by format_direct_children_list,
                // no need to recurse into them here.
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
            // which rclone lsjson sometimes includes with empty Path/Name.
            if raw_file.path.is_empty() && raw_file.name.is_empty() && raw_file.is_dir {
                continue; // Skip rclone's potential root marker entry
            }
            // Also skip entries where path is unexpectedly empty
            if raw_file.path.is_empty() {
                eprintln!("Warning: Skipping raw file with empty path: {:?}", raw_file);
                continue;
            }
            // Allow empty Name for directories if Path is set (e.g., top-level dir listed like "mydir/")
            if !raw_file.is_dir && raw_file.name.is_empty() {
                // But files must have a name
                eprintln!("Warning: Skipping raw file with empty name: {:?}", raw_file);
                continue;
            }

            // Convert RawFile to the internal File representation
            let file = File::from_raw(service_name.to_string(), &raw_file);
            let key = file.get_key();

            // Add file hashes to map for duplicate checking later
            if !file.is_dir {
                if let Some(hashes) = &file.hashes {
                    // Only consider files with at least one hash type present
                    // (Checking for None Option is done by let Some)
                    // Further filter by specific hash types if needed (e.g., require SHA1 or MD5)
                    if file.size >= 0 {
                        // Only consider files with non-negative size for duplicate checks
                        self.hash_map
                            .entry(hashes.clone()) // Clone hashes to use as key
                            .or_default() // Get vec or create new one
                            .push(key.clone()); // Add the file's key to the list for this hash
                    }
                }
            }

            // Store the file/dir; warn on overwrite (might indicate duplicate input or key collision)
            // Attempt to insert the file; check if a value was already present for this key
            if self.files.insert(key.clone(), file.clone()).is_some() {
                // If insert returned Some, a file with this key already existed.
                // It's useful to check if the type (dir/file) is different, which indicates a problem.
                if let Some(existing_file) = self.files.get(&key) {
                    // Check if the existing file's directory status differs from the new one
                    if existing_file.is_dir != file.is_dir {
                        eprintln!("Warning: Overwriting existing key '{}' with different type (is_dir: {} -> {}). Check rclone output consistency.", key, existing_file.is_dir, file.is_dir);
                    }
                    // We might log other differences here too if needed (e.g., size, modtime).
                }
                // Re-insert the new file regardless of warning, assuming the latest data is preferred.
                self.files.insert(key.clone(), file);
            }
            // Keep track of all keys added for this specific service run
            service_keys_added_this_run.insert(key);
        }

        // Determine root keys for this service after processing all its files
        // Filter the keys added in this run:
        // 1. Get the corresponding File object from the main `files` map.
        // 2. Keep only those files whose `get_parent_key()` returns None (meaning they are roots).
        // 3. Collect their keys into a vector.
        let service_root_keys: Vec<String> = service_keys_added_this_run
            .iter()
            .filter_map(|key| self.files.get(key)) // Get the File object for the key
            .filter(|file| file.get_parent_key().is_none()) // Check if it has no parent
            .map(|file| file.get_key()) // Get the key of the root file
            .collect();

        // Store the identified root keys for this service
        self.roots_by_service
            .insert(service_name.to_string(), service_root_keys);

        println!(
            "Finished adding {} files/dirs for remote {}",
            service_keys_added_this_run.len(), // Count of items processed for this service
            service_name
        );
    }

    /// Builds the parent-child relationships between `File` objects.
    /// Iterates through all files, determines their parent key, and updates the
    /// parent's `children_keys` set. Should be called after all files from all
    /// services have been added via `add_remote_files`.
    pub fn build_tree(&mut self) {
        println!("Building tree structure...");
        // Temporary map to store parent key -> set of child keys relationships
        let mut parent_child_map: HashMap<String, HashSet<String>> = HashMap::new();

        // First pass: Collect all parent-child relationships based on keys
        // Get a list of all keys currently in the main `files` map
        let all_keys: Vec<String> = self.files.keys().cloned().collect();

        // Iterate through all keys to find parent-child links
        for key in all_keys {
            if let Some(file) = self.files.get(&key) {
                // Try to get the parent key for the current file
                if let Some(parent_key) = file.get_parent_key() {
                    // Check if the potential parent exists in our map
                    if let Some(parent_file) = self.files.get(&parent_key) {
                        // Ensure the parent is actually a directory
                        if parent_file.is_dir {
                            // Add the current file's key to the parent's entry in the temporary map
                            parent_child_map
                                .entry(parent_key.clone()) // Get or insert the parent key
                                .or_default() // Get the HashSet or create a new one
                                .insert(key.clone()); // Add the child key to the set
                        } else {
                            // This indicates an inconsistency, possibly in key generation or source data.
                            eprintln!(
                                "Warning: Potential parent key '{}' for child '{}' exists but is not marked as a directory. Cannot assign child.",
                                parent_key, key
                            );
                        }
                    } else {
                        // Parent key was generated, but no corresponding entry found in `files`.
                        // This might happen if rclone didn't list the parent directory itself.
                        eprintln!(
                            "Warning: Parent key '{}' not found for file '{}'. File might be orphaned in tree view.",
                            parent_key, key
                        );
                        // Consider adding orphaned items as roots? For now, just warn.
                    }
                }
                // If get_parent_key returned None, it's a root node. Roots were already identified
                // in add_remote_files and stored in roots_by_service.
            }
        }

        // Second pass: Update the actual `children_keys` field in the parent File objects
        for (parent_key, children_keys) in parent_child_map {
            // Get a mutable reference to the parent File object
            if let Some(parent_file) = self.files.get_mut(&parent_key) {
                // We already verified parent_file.is_dir in the first pass
                // Assign the collected set of child keys to the parent's children_keys field
                parent_file.children_keys = children_keys;
            }
            // If parent_file is somehow not found here (though it should exist), the relationships are lost,
            // but we avoid a panic. This shouldn't happen if the first pass logic is correct.
        }
        println!("Tree structure built.");
    }

    /// Identifies duplicate files based on the `hash_map` populated during `add_remote_files`.
    /// Populates the `duplicates` map with `DuplicateInfo` for sets of files sharing the same hash.
    /// Only considers files with valid hashes and size >= 0.
    pub fn find_duplicates(&mut self) {
        println!("Finding duplicates based on hashes...");
        self.duplicates.clear(); // Clear previous results
                                 // Iterate through the map of hash -> list of file keys
        for (hash, keys) in &self.hash_map {
            if keys.len() > 1 {
                // Need at least 2 files sharing the same hash to be considered duplicates
                let mut paths = Vec::new(); // To store formatted paths like "service:path/to/file"
                let mut size: Option<u64> = None; // To store the size (should be consistent for same hash)
                let mut valid_files_found = 0; // Count how many actual files we find for this hash

                // Iterate through the keys associated with the current hash
                for key in keys {
                    if let Some(file) = self.files.get(key) {
                        // Ensure it's a file (not a dir) and has a non-negative size
                        if !file.is_dir && file.size >= 0 {
                            // Format the path string for the report
                            let service_and_path =
                                format!("{}:{}", file.service, file.path.join("/"));
                            paths.push(service_and_path);
                            valid_files_found += 1;

                            // Store the size of the first valid file found
                            if size.is_none() {
                                size = Some(file.size as u64);
                            } else if size != Some(file.size as u64) {
                                // If subsequent files with the same hash have different sizes, log a warning.
                                // This indicates a potential issue with rclone's reporting or the hashing itself.
                                eprintln!(
                                    "Warning: Files with same hash {:?} have different sizes reported: {} vs {} (Key: {})",
                                    hash,
                                    human_bytes(size.unwrap() as f64), // Display human-readable size
                                    human_bytes(file.size as f64), // Display human-readable size
                                    key
                                );
                                //FIXME Decide how to handle: skip this set, use first size, etc.
                                // Current behavior: Keep the first size found and continue.
                            }
                        }
                    }
                }

                // Store the duplicate info if we found multiple valid files and have a size
                if valid_files_found > 1 && size.is_some() {
                    paths.sort(); // Sort paths alphabetically for consistent report output
                    self.duplicates.insert(
                        hash.clone(), // Use the hash as the key in the duplicates map
                        DuplicateInfo {
                            paths,                   // Store the sorted list of paths
                            size: size.unwrap_or(0), // Store the determined size (default to 0 if None somehow)
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
        // Use RefCell for interior mutability, allowing the cache to be modified
        // within the recursive helper function even though `self` is borrowed immutably.
        let size_cache = RefCell::new(HashMap::new());

        // Recursive helper function to calculate size and utilize the cache.
        fn calculate_recursive_and_cache(
            key: &str,                             // The key of the current node (file or directory)
            all_files: &HashMap<String, File>,     // The main map of all files/dirs
            cache: &RefCell<HashMap<String, u64>>, // Reference to the mutable cache
        ) -> u64 {
            // --- 1. Check cache ---
            // Try to borrow the cache immutably to check if the size for this key exists.
            if let Some(&cached_size) = cache.borrow().get(key) {
                // If found, return the cached size immediately.
                return cached_size;
            }

            // --- 2. Calculate size (if not in cache) ---
            let calculated_size = if let Some(file) = all_files.get(key) {
                // Get the File object for the current key.
                if !file.is_dir {
                    // It's a file: Use its size directly.
                    // Handle rclone's potential -1 size for unknown/pending files.
                    if file.size >= 0 {
                        file.size as u64
                    } else {
                        0 // Treat negative sizes as 0 for total calculation.
                    }
                } else {
                    // It's a directory: Sum the sizes of its children recursively.
                    let mut total_size: u64 = 0;
                    // Iterate over the keys of the direct children of this directory.
                    for child_key in &file.children_keys {
                        // Recursive call to calculate the size of each child.
                        // This will either return a cached value or calculate and cache it.
                        total_size += calculate_recursive_and_cache(child_key, all_files, cache);
                    }
                    total_size // The calculated size for the directory is the sum of its children.
                }
            } else {
                // This should not happen if the tree structure is consistent.
                eprintln!("Warning: Key '{}' not found during size calculation.", key);
                0 // Return 0 if key is somehow missing to avoid panics.
            };

            // --- 3. Store in cache ---
            // Borrow the cache mutably to insert the newly calculated size.
            cache.borrow_mut().insert(key.to_string(), calculated_size);

            // --- 4. Return calculated size ---
            calculated_size
        }

        // --- Main calculation loop ---
        // Iterate through the root nodes of each service to initiate the recursive calculation.
        for service_name in self.roots_by_service.keys() {
            let mut service_total_size: u64 = 0;
            // Get the list of root keys for the current service.
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned() // Clone the Vec<String>
                .unwrap_or_default(); // Use an empty Vec if service not found (shouldn't happen)

            // Calculate the size for each root node in the service.
            // The recursive helper will populate the cache as it goes.
            for root_key in &root_keys {
                service_total_size +=
                    calculate_recursive_and_cache(root_key, &self.files, &size_cache);
            }
            // Add the service's total size to the grand total.
            grand_total_size += service_total_size;
            // Store the total size for this service.
            service_sizes.insert(service_name.clone(), service_total_size);
        }

        // Return the overall total, per-service totals, and the filled cache.
        // `into_inner()` consumes the RefCell and returns the owned HashMap.
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
        size_cache: &HashMap<String, u64>, // Pass in the pre-calculated size cache
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
        service_sizes: &HashMap<String, u64>, // Pass in pre-calculated service totals
    ) -> String {
        let mut final_text: Vec<String> = Vec::new();

        // Sort services alphabetically for consistent output order
        let mut sorted_services: Vec<&String> = self.roots_by_service.keys().collect();
        sorted_services.sort();

        for service_name in sorted_services {
            // Get the pre-calculated total size for this service
            let service_total_size = service_sizes.get(service_name).cloned().unwrap_or(0);
            let mut service_entries_lines: Vec<String> = Vec::new();

            // Get the root keys for this service
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
                        // .reverse() // Directories first
                        .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                }
            });

            // Format each root node and its descendants recursively
            for root_key in &sorted_root_keys {
                if let Some(root_file) = self.files.get(root_key) {
                    let indent_size_per_level = 2; // Standard indent for tree view
                                                   // Call format_tree_entry, which uses the size_cache internally
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
                    // Log if a listed root key is not actually found in the files map
                    eprintln!(
                        "Warning: Root key '{}' not found for service '{}'.",
                        root_key, service_name
                    );
                }
            }

            // Add the service header line
            final_text.push(format!(
                "{} {}: {}",
                remote_icon,
                service_name,
                human_bytes(service_total_size as f64) // Display human-readable total size
            ));
            // Add the formatted tree lines for this service
            final_text.push(service_entries_lines.join("\n"));
            final_text.push("".to_string()); // Add a blank line between services
        }

        // Join all parts into a single string
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
        size_cache: &HashMap<String, u64>, // Pass in the pre-calculated size cache
        folder_icon: &str,
        file_icon: &str,
        size_icon: &str,
        date_icon: &str,
        remote_icon: &str,
    ) -> (String, u64) {
        // FIXME Still returns total size for consistency if needed upstream
        let mut service_total_size: u64 = 0; // Initialize total size for this service
        let mut service_entries_lines: Vec<String> = Vec::new();

        // Get the root keys for the specified service
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
                    // .reverse() // Directories first
                    .then_with(|| a.get_display_name().cmp(&b.get_display_name())),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => Ordering::Equal,
            }
        });

        // Format each root node and its descendants
        for root_key in &sorted_root_keys {
            if let Some(root_file) = self.files.get(root_key) {
                // Add this root's cached size to the service total
                // Retrieve the pre-calculated size for this root node from the cache
                service_total_size += size_cache.get(root_key).cloned().unwrap_or(0);

                let indent_size_per_level = 2;
                // Recursively format the entry using the cache
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

        // Create the header line with the service name and its total calculated size
        let header = format!(
            "{} {}: {}",
            remote_icon,
            service_name,
            human_bytes(service_total_size as f64) // Display human-readable total
        );
        // Join the formatted tree lines
        let body = service_entries_lines.join("\n");

        // Return the combined header and body string, and the calculated total size
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

        // Convert the BTreeMap iterator to a Vec for parallel sorting
        let mut sorted_duplicates: Vec<(&Hashes, &DuplicateInfo)> =
            self.duplicates.iter().collect();

        // Use rayon's parallel sort for potentially large lists of duplicates.
        // Sorts unstable (doesn't preserve order of equal elements, but faster).
        // Sort primarily by potential wasted space (descending), then by size (desc), then by count (desc).
        sorted_duplicates.par_sort_unstable_by(|a, b| {
            // Calculate "wasted" space = size * (number of copies - 1)
            let wasted_a = a.1.size * (a.1.paths.len().saturating_sub(1)) as u64;
            let wasted_b = b.1.size * (b.1.paths.len().saturating_sub(1)) as u64;
            wasted_b // Primary sort: Wasted space descending
                .cmp(&wasted_a)
                // Secondary sort: File size descending (larger files first if wasted space is equal)
                .then_with(|| b.1.size.cmp(&a.1.size))
                // Tertiary sort: Number of duplicate paths descending (more copies first if size is also equal)
                .then_with(|| b.1.paths.len().cmp(&a.1.paths.len()))
        });

        let mut lines: Vec<String> = Vec::new();
        let mut total_potential_savings: u64 = 0;
        let mut total_duplicate_size: u64 = 0; // Total size occupied by all copies of duplicate files

        // Pre-calculate summary totals
        for (_hashes, info) in &sorted_duplicates {
            let num_files = info.paths.len();
            if num_files > 1 {
                // Calculate size occupied by all instances of this duplicate set
                let current_duplicate_set_total_size = info.size * num_files as u64;
                // Calculate potential saving if all but one copy are removed
                let potential_saving = info.size * (num_files.saturating_sub(1)) as u64;
                total_duplicate_size += current_duplicate_set_total_size;
                total_potential_savings += potential_saving;
            }
        }

        // Add summary information at the top of the report
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

        // Add details for each duplicate set
        for (hashes, info) in sorted_duplicates {
            let num_duplicates = info.paths.len();
            let potential_saving = info.size * (num_duplicates.saturating_sub(1)) as u64; // Saving = size * (n-1)

            // Add header for this duplicate set
            lines.push(format!(
                "\nDuplicates found with size: {} ({} files, potential saving: {})",
                human_bytes(info.size as f64), // Size of each file
                num_duplicates,                // How many copies found
                human_bytes(potential_saving as f64)  // Potential saving for this set
            ));

            // List the paths where these duplicates were found
            for path_key in &info.paths {
                lines.push(format!("  - {}", path_key)); // Indent paths for readability
            }

            // List the specific hashes that matched for this set
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
        // Join all lines into the final report string
        lines.join("\n")
    }

    /// Generates the formatted text report summarizing file counts and total sizes per extension.
    /// Uses parallel sorting (`rayon`) to order extensions primarily by total size (descending).
    ///
    /// # Returns
    /// A string containing the formatted extensions report.
    pub fn generate_extensions_report(&self) -> String {
        println!("Generating extensions report...");
        // Map to store aggregated data: extension -> (count, total_size)
        let mut extensions_map: HashMap<String, (u64, u64)> = HashMap::new();
        let mut total_file_count: u64 = 0;
        let mut total_files_size: u64 = 0;

        // Iterate through all files in the collection
        for file in self.files.values() {
            // Only consider actual files with non-negative size
            if !file.is_dir && file.size >= 0 {
                total_file_count += 1;
                let file_size = file.size as u64;
                total_files_size += file_size;

                // Determine the key for the map (extension or special key for no extension)
                let ext_key = if file.ext.is_empty() {
                    NO_EXTENSION_KEY.to_string() // Use the constant for files without extensions
                } else {
                    file.ext.clone() // Use the lowercase extension stored in the File struct
                };

                // Get or insert the entry for this extension and update count and size
                let entry = extensions_map.entry(ext_key).or_insert((0, 0));
                entry.0 += 1; // Increment count
                entry.1 += file_size; // Add to total size
            }
        }

        if extensions_map.is_empty() {
            // Handle the case where no files were processed or found
            return "No files found to generate extensions report.".to_string();
        }

        // Convert the map to a Vec of ExtensionInfo structs for sorting
        let mut sorted_extensions: Vec<ExtensionInfo> = extensions_map
            .into_iter()
            .map(|(ext, (count, size))| ExtensionInfo {
                extension: ext,
                count,
                total_size: size,
            })
            .collect();

        // Use parallel sort for potentially many extensions.
        // Sort primarily by Total Size (descending), then Count (descending), then Extension Name (ascending).
        sorted_extensions.par_sort_unstable_by(|a, b| {
            b.total_size // Primary: Size Descending
                .cmp(&a.total_size)
                .then_with(|| b.count.cmp(&a.count)) // Secondary: Count Descending
                .then_with(|| a.extension.cmp(&b.extension)) // Tertiary: Extension Name Ascending (alphabetical)
        });

        let mut lines: Vec<String> = Vec::new();
        // Add summary header
        lines.push(format!(
            "Total Files Found: {} (Total Size: {})",
            total_file_count,
            human_bytes(total_files_size as f64)
        ));
        lines.push("-------------------------------------".to_string());
        // Add table header row
        lines.push(format!(
            // Adjust column widths as needed
            "{:<15} | {:>15} | {:>12} | {:>8} | {:>8}",
            "Extension", "Total Size", "File Count", "% Size", "% Count"
        ));
        lines.push(
            "--------------------------------------------------------------------".to_string(), // Separator line
        );

        // Add data rows for each extension info
        for info in sorted_extensions {
            // Calculate percentage of total count
            let count_percent = if total_file_count > 0 {
                (info.count as f64 / total_file_count as f64) * 100.0
            } else {
                0.0
            };
            // Calculate percentage of total size
            let size_percent = if total_files_size > 0 {
                (info.total_size as f64 / total_files_size as f64) * 100.0
            } else {
                0.0
            };
            // Format extension name for display (add leading dot, handle NO_EXTENSION_KEY)
            let ext_display = if info.extension == NO_EXTENSION_KEY {
                info.extension.clone()
            } else {
                format!(".{}", info.extension) // Prepend "." for actual extensions
            };

            // Format the data row according to the header columns
            lines.push(format!(
                "{:<15} | {:>15} | {:>12} | {:>7.2}% | {:>7.2}%", // Use ".2" for two decimal places in percentages
                ext_display,                                      // Extension name
                human_bytes(info.total_size as f64),              // Human-readable total size
                info.count,                                       // File count
                size_percent,                                     // Size percentage
                count_percent                                     // Count percentage
            ));
        }
        // Join all lines into the final report string
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
        if num_largest_files == 0 {
            // This case should be handled by the caller, but defensive.
            return "Largest files report disabled (count is 0).".to_string();
        }

        println!("Identifying top {} largest files...", num_largest_files);

        let mut all_actual_files: Vec<&File> = self
            .files
            .values()
            .filter(|f| !f.is_dir && f.size >= 0) // Only actual files with valid non-negative size
            .collect();

        if all_actual_files.is_empty() {
            return "No files found to generate largest files report.".to_string();
        }

        // Sort files by size in descending order.
        // Using parallel sort for potentially very large number of total files.
        all_actual_files.par_sort_unstable_by(|a, b| b.size.cmp(&a.size));

        let mut lines: Vec<String> = Vec::new();
        let actual_num_to_show = num_largest_files.min(all_actual_files.len());

        lines.push(format!(
            "Top {} Largest Files (across all remotes):",
            actual_num_to_show
        ));
        lines.push(
            "----------------------------------------------------------------------".to_string(),
        ); // Adjusted separator
        lines.push(format!(
            "{:<5} | {:>15} | {}", // Adjusted formatting for Rank, Size, Path
            "Rank", "Size", "Path (Service:File)"
        ));
        lines.push(
            "----------------------------------------------------------------------".to_string(),
        ); // Adjusted separator

        for (i, file) in all_actual_files.iter().take(actual_num_to_show).enumerate() {
            let rank = i + 1;
            let size_str = human_bytes(file.size as f64);
            // Construct full path: service:path/components...
            // file.path contains components like ["dir", "file_stem_if_ext_present_else_name"]
            // file.name (from RawFile.Name) is the base name.
            // file.ext is the extension.
            // The path stored in `file.path` should represent the full path relative to the service root,
            // with the last component being the filename (possibly without extension depending on from_raw).
            // Let's assume file.path.join("/") correctly reconstructs the full relative path.
            let full_path = format!("{}:{}", file.service, file.path.join("/"));

            lines.push(format!(
                "{:<5} | {:>15} | {}", // Consistent with header
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
        // Get keys (service names) from the roots_by_service map
        let mut names: Vec<String> = self.roots_by_service.keys().cloned().collect();
        names.sort(); // Sort alphabetically
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
        size_cache: &HashMap<String, u64>, // Pass in pre-calculated sizes
        service_sizes: &HashMap<String, u64>, // Pass in pre-calculated service totals
    ) -> Vec<EChartsTreemapNode> {
        println!("Generating data for ECharts treemap...");
        let mut treemap_data: Vec<EChartsTreemapNode> = Vec::new();

        // Recursive helper function to build the nested ECharts node structure.
        fn build_echarts_node_recursive(
            key: &str,                         // Key of the current File node
            all_files: &HashMap<String, File>, // Map of all File objects
            size_cache: &HashMap<String, u64>, // Pre-calculated sizes
        ) -> Option<EChartsTreemapNode> {
            if let Some(file) = all_files.get(key) {
                let name = file.get_display_name(); // Get name for the treemap node
                                                    // Use pre-calculated size from cache, default to 0 if missing
                let size = size_cache.get(key).cloned().unwrap_or(0);

                if !file.is_dir {
                    // It's a file: return a leaf node if it has size > 0.
                    if size > 0 {
                        Some(EChartsTreemapNode {
                            name,
                            value: size,    // Value is the file size
                            children: None, // Files have no children
                        })
                    } else {
                        None // Exclude 0-byte files from treemap visualization for clarity
                    }
                } else {
                    // It's a directory: recursively process its children.
                    let mut children_nodes: Vec<EChartsTreemapNode> = Vec::new();

                    // Sort children keys for deterministic treemap layout (optional but recommended).
                    // Sort: Dirs first, then Files, then alphabetically by display name.
                    let mut sorted_children_keys: Vec<&String> =
                        file.children_keys.iter().collect();
                    sorted_children_keys.sort_by(|a_key, b_key| {
                        match (all_files.get(*a_key), all_files.get(*b_key)) {
                            (Some(a), Some(b)) => a
                                .is_dir
                                .cmp(&b.is_dir) // Compare is_dir status
                                .reverse() // Reverse to put directories (true) before files (false)
                                .then_with(|| a.get_display_name().cmp(&b.get_display_name())), // Then sort alphabetically
                            _ => Ordering::Equal, // Handle missing keys (should not happen ideally)
                        }
                    });

                    // Recursively build nodes for each child
                    for child_key in sorted_children_keys {
                        if let Some(child_node) =
                            build_echarts_node_recursive(child_key, all_files, size_cache)
                        {
                            // Only add child nodes that were successfully created (e.g., non-zero size files)
                            children_nodes.push(child_node);
                        }
                    }

                    // Only include the directory node itself if it has a non-zero calculated size
                    // OR if it contains children that have sizes (i.e., it's not an empty branch).
                    if size > 0 || !children_nodes.is_empty() {
                        Some(EChartsTreemapNode {
                            name,
                            value: size, // Value is the directory's total calculated size
                            children: if children_nodes.is_empty() {
                                None
                            } else {
                                Some(children_nodes)
                            }, // Assign children if any exist
                        })
                    } else {
                        None // Exclude empty directories (size 0 and no non-empty children) from treemap
                    }
                }
            } else {
                // Log if a key is encountered that doesn't exist in the main map
                eprintln!(
                    "Warning: Key '{}' not found during ECharts data generation.",
                    key
                );
                None // Return None if the key is invalid
            }
        }

        // --- Main ECharts data generation loop ---
        // Sort service names for consistent output order at the top level of the treemap
        let mut sorted_service_names: Vec<&String> = self.roots_by_service.keys().collect();
        sorted_service_names.sort();

        // Iterate through each service
        for service_name in sorted_service_names {
            let mut service_children: Vec<EChartsTreemapNode> = Vec::new();
            // Get the root keys for the current service
            let root_keys = self
                .roots_by_service
                .get(service_name)
                .cloned()
                .unwrap_or_default();

            // Sort root keys for deterministic output (Dirs first, then files, then alpha)
            let mut sorted_root_keys = root_keys;
            sorted_root_keys.sort_by(|a_key, b_key| {
                match (self.files.get(a_key), self.files.get(b_key)) {
                    (Some(a), Some(b)) => a
                        .is_dir
                        .cmp(&b.is_dir) // Dirs before files
                        .reverse() // reverse bool comparison
                        .then_with(|| a.get_display_name().cmp(&b.get_display_name())), // Then alphabetical
                    _ => Ordering::Equal, // Handle missing keys (should not happen ideally)
                }
            });

            // Recursively build ECharts nodes starting from each root key of the service
            for root_key in &sorted_root_keys {
                if let Some(root_node) =
                    build_echarts_node_recursive(root_key, &self.files, size_cache)
                {
                    // Add valid root nodes (and their descendants) to the service's children list
                    service_children.push(root_node);
                }
            }

            // Get the total pre-calculated size for this service
            let service_total_size = service_sizes.get(service_name).cloned().unwrap_or(0);

            // Add the service node itself to the final treemap data,
            // but only if it has a non-zero size or contains children.
            if service_total_size > 0 || !service_children.is_empty() {
                treemap_data.push(EChartsTreemapNode {
                    name: service_name.clone(), // Name is the service name
                    value: service_total_size,  // Value is the total size of the service
                    children: if service_children.is_empty() {
                        None
                    } else {
                        Some(service_children)
                    }, // Assign children if any exist
                });
            }
        }

        println!("ECharts data generation complete.");
        treemap_data // Return the final vector of top-level service nodes
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
    title: &str, // Original title passed from main.rs for the <title> tag
) -> Result<String, serde_json::Error> {
    // Serialize the Rust data structure to a JSON string for embedding in JavaScript
    let data_json = serde_json::to_string(data)?;

    // Define a color palette for ECharts levels/nodes
    let colors_json = serde_json::to_string(&[
        "#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de", "#3ba272", "#fc8452", "#9a60b4",
        "#ea7ccc",
    ])?; // Serialize the color array to JSON

    // Define the specific title text to display within the chart itself
    let specific_title =
        "Cloud Storage Treemap - Hover over items to see more info. Click to drill down.";

    // Create the HTML structure using a format string.
    // Embeds the JSON data, color palette, titles, and ECharts initialization script.
    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title> 
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script> 
    <style>
        /* Basic styling for full-page chart and dark theme */
        html, body {{ height: 100%; margin: 0; padding: 0; overflow: hidden; }}
        #main {{ height: 100%; width: 100%; background-color: #000000; }} /* Chart container */
        .tooltip-title {{ font-weight: bold; margin-bottom: 5px; }} /* Style for tooltip title */
    </style>
</head>
<body>
    <div id="main"></div> 
    <script type="text/javascript">
        var chartDom = document.getElementById('main');
        var myChart = echarts.init(chartDom, 'dark'); // Initialize ECharts instance with 'dark' theme
        var option;

        var data = {data_json}; // Inject the serialized Rust data here
        var colors = {colors_json}; // Inject the serialized color palette here

        // Helper function within JS to format bytes (KiB, MiB, GiB, etc.)
        function formatBytes(bytes, decimals = 2) {{
            if (!+bytes) return '0 Bytes' // Handle 0 or invalid input
            const k = 1024 // Use 1024 for KiB, MiB, etc.
            const dm = decimals < 0 ? 0 : decimals
            const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
            // Calculate the appropriate size index (0 for Bytes, 1 for KiB, etc.)
            const i = Math.max(0, Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k))));
            // Format the number with specified decimals and append the size unit
            return `${{parseFloat((bytes / Math.pow(k, i)).toFixed(dm))}} ${{sizes[i]}}`
        }}

        // ECharts configuration object
        option = {{
            title: {{
                text: '{specific_title}', // Use the specific title text for the chart display
                left: 'center', // Center the title
                textStyle: {{ color: '#fff' }} // White text suitable for dark theme
            }},
            // Assign the color palette to the series
            color: colors,
            tooltip: {{ // Configure tooltips on hover
                formatter: function (info) {{ // Custom formatter function
                    var value = info.value; // The node's size value
                    var treePathInfo = info.treePathInfo; // Array of ancestor nodes including self
                    var treePath = [];
                    // Build the path string (e.g., "service/folder/file") from the treePathInfo
                    // Start from index 1 to skip the invisible root node ECharts adds internally
                    for (var i = 1; i < treePathInfo.length; i++) {{
                        treePath.push(treePathInfo[i].name);
                    }}
                    // Return formatted HTML for the tooltip content
                    return '<div class="tooltip-title">' + echarts.format.encodeHTML(treePath.join('/')) + '</div>' // Display path as title
                         + 'Size: ' + formatBytes(value); // Display formatted size
                }}
            }},
            series: [
                {{
                    name: 'Cloud Storage', // Series name
                    type: 'treemap', // Chart type
                    roam: true, // Enable panning (drag) and zooming (scroll)
                    nodeClick: 'zoomToNode', // Zoom into a node when clicked
                    squareRatio: 0.8, // Adjust ratio towards squares (lower value = more rectangular)
                    colorSaturation: [0.5, 0.9], // Range for color saturation variation
                    breadcrumb: {{ // Configure the navigation breadcrumb trail at the bottom
                        show: true, // Display the breadcrumb
                        height: 22, // Height of the breadcrumb bar
                        left: 'center', // Center the breadcrumb
                        bottom: '5%', // Position near the bottom
                         itemStyle: {{ // Styling for breadcrumb items
                            textStyle: {{
                                color: '#fff' // White text for dark theme
                            }}
                         }},
                         emphasis: {{ // Styling when hovering over breadcrumb items
                            itemStyle: {{
                                textStyle: {{ color: '#ddd' }} // Slightly dimmer white on hover
                            }}
                         }}
                    }},
                    label: {{ // Default label settings (for leaf nodes or nodes without children shown)
                        show: false, // Hide labels by default to avoid clutter
                        // Other settings kept from example but inactive due to show: false
                        position: 'inside',
                        formatter: function (params) {{ // How labels would be formatted if shown
                            return params.name + '\n' + formatBytes(params.value);
                         }},
                         color: '#fff', // Label color if shown
                         overflow: 'truncate', // Truncate labels if they overflow node bounds
                    }},
                    upperLabel: {{ // Configuration for labels of parent nodes when drilled down
                        show: false, // Hide upper labels as well
                        // Other settings kept from example but inactive due to show: false
                        color: '#fff',
                    }},
                    itemStyle: {{ // Default style for treemap nodes (borders, gaps)
                        borderColor: '#000', // Black border to blend with dark background
                        borderWidth: 0.5, // Very thin border
                        gapWidth: 0.0, // No gap between nodes for maximum space usage
                    }},
                    levels: [ // Define specific styles for different levels of the hierarchy
                        {{ // Level 0 (ECharts depth 1): Top-level services
                            itemStyle: {{ borderColor: '#ff6361', borderWidth: 15, gapWidth: 0, borderRadius: 15 }} // Subtle border, small gap
                        }},
                        {{ // Level 1 (ECharts depth 2): First level folders/files within a service
                            itemStyle: {{ borderColor: '#fff', borderWidth: 15, gapWidth: 0, borderRadius: 15 }} // Thinner border, smaller gap
                        }},
                         // Deeper levels (Level 2+) will use the default itemStyle defined above
                         // (thin black border, no gap) unless more levels are specified here.
                    ],
                    data: data // Assign the injected JSON data to the series
                }}
            ]
        }};

        // Apply the configuration to the chart instance
        myChart.setOption(option);

        // Add event listener to resize the chart when the window is resized
        window.addEventListener('resize', myChart.resize);

    </script>
</body>
</html>"#,
        title = title,                   // Use original title for <title> tag
        specific_title = specific_title, // Use specific text for chart title H1
        data_json = data_json,           // Embed the data JSON string
        colors_json = colors_json        // Embed the colors JSON string
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
    // Format arguments for display, quoting if they contain spaces
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
    // Reduced noisy printing during parallel runs, log start from caller if needed
    // println!("> Running: {} {}", executable, args_display);

    // Configure and run the command
    let output_result = Command::new(executable)
        .args(args) // Pass the arguments
        .stdout(Stdio::piped()) // Capture standard output
        .stderr(Stdio::piped()) // Capture standard error
        .output(); // Execute the command and wait for it to finish

    // Handle the result of command execution
    match output_result {
        Ok(ref out) => {
            // Command executed, check if it was successful (exit code 0)
            if !out.status.success() {
                // Command failed, log the error details
                // eprintln!("  Command failed with status: {}", out.status); // Reduced verbosity
                let stderr_str = String::from_utf8_lossy(&out.stderr); // Get stderr content
                if !stderr_str.is_empty() {
                    // Log the command and its stderr output if available
                    eprintln!(
                        "  Command '{} {}' failed with stderr:\n---\n{}\n---",
                        executable,
                        args_display,      // Display the formatted command arguments
                        stderr_str.trim()  // Trim whitespace from stderr
                    );
                } else {
                    // Log failure status code if no stderr output was captured
                    eprintln!(
                        "  Command '{} {}' failed with status code {}. No stderr output.",
                        executable, args_display, out.status
                    );
                }
                // Note: Even on command failure (non-zero exit code), Ok(out) is returned here.
                // The caller needs to check output.status.success().
            }
            // Return the Output object whether the command succeeded or failed execution-wise
            // Needs Ok() wrapping because we matched on output_result which was Result<Output, io::Error>
            Ok(out.clone()) // Clone the output to return (or return the original `out` if `output_result` is consumed)
        }
        Err(ref e) => {
            // Failed to even start the command (e.g., not found, permissions)
            eprintln!("  Failed to execute command '{}': {}", executable, e);
            // Provide a hint if the error is "NotFound"
            if e.kind() == ErrorKind::NotFound {
                eprintln!(
                    "  Hint: Make sure '{}' is installed and in your system's PATH, or provide the full path.",
                    executable
                );
            }
            // Return the original io::Error
            Err(io::Error::new(e.kind(), e.to_string())) // Clone or recreate the error if needed
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
    // Ensure the output directory exists.
    fs::create_dir_all(output_dir)?;

    // --- 1. Build parent-child links ---
    // This step establishes the hierarchical relationships based on paths.
    files_data.build_tree();

    // --- 2. Find and report duplicates (if enabled) ---
    let duplicates_output_path = output_dir.join(duplicates_output_filename);
    if enable_duplicates_report {
        // Perform duplicate detection based on previously collected hashes.
        files_data.find_duplicates();
        // Generate the report string (uses parallel sort internally).
        let duplicates_report_string = files_data.generate_duplicates_output();
        // Write the report to the specified file.
        fs::write(&duplicates_output_path, duplicates_report_string)?;
        println!(
            "Duplicates report written to '{}'",
            duplicates_output_path.display()
        );
    } else {
        println!("Duplicate detection skipped.");
        // If disabled, remove any pre-existing duplicates report file.
        if duplicates_output_path.exists() {
            let _ = fs::remove_file(&duplicates_output_path);
        }
    }

    // --- 2b. Generate and report extensions (if enabled) ---
    let extensions_output_path = output_dir.join(extensions_output_filename);
    if enable_extensions_report {
        // Generate the extensions report string (uses parallel sort internally).
        let extensions_report_string = files_data.generate_extensions_report();
        // Write the report to the specified file.
        fs::write(&extensions_output_path, extensions_report_string)?;
        println!(
            "Extensions report written to '{}'",
            extensions_output_path.display()
        );
    } else {
        println!("Extensions report skipped.");
        // If disabled, remove any pre-existing extensions report file.
        if extensions_output_path.exists() {
            let _ = fs::remove_file(&extensions_output_path);
        }
    }

    // --- 2c. Generate Largest Files Report (if enabled) ---
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

    // --- 3. Calculate sizes and populate cache (runs once) ---
    // This is crucial as subsequent steps rely on the calculated sizes and cache.
    println!("Calculating sizes and populating cache...");
    // Perform the recursive size calculation, obtaining totals and the cache.
    let (grand_total_size, service_sizes, size_cache) = files_data.calculate_all_sizes_with_cache();
    println!("Size calculation complete.");

    // --- 4. Generate Size Report (using calculated totals) ---
    let size_output_path = output_dir.join(size_output_filename);
    let mut size_report_lines: Vec<String> = Vec::new();
    // Add grand total line first.
    size_report_lines.push(format!(
        "Total used size across all services (calculated from listings): {}",
        human_bytes(grand_total_size as f64) // Display human-readable grand total
    ));
    size_report_lines.push("".to_string()); // Blank line

    // Add per-service totals, sorted alphabetically.
    let mut sorted_service_names: Vec<&String> = service_sizes.keys().collect();
    sorted_service_names.sort();
    for service_name in sorted_service_names {
        if let Some(size) = service_sizes.get(service_name) {
            size_report_lines.push(format!("{}: {}", service_name, human_bytes(*size as f64)));
        }
    }
    size_report_lines.push("".to_string()); // Blank line
                                            // Repeat grand total at the end for visibility.
    size_report_lines.push(format!(
        "Total used size across all services (calculated from listings): {}",
        human_bytes(grand_total_size as f64)
    ));
    // Join lines and write the report file.
    let size_report_string = size_report_lines.join("\n");
    fs::write(&size_output_path, &size_report_string)?;
    println!("Size report written to '{}'", size_output_path.display());

    // --- 5. Generate Tree/File Structure Report (Uses size_cache) ---
    // The output format depends on the selected OutputMode.
    match output_division_mode {
        OutputMode::Single => {
            // Generate a single string containing the tree for all services.
            let tree_report_string = files_data.generate_full_tree_string(
                &size_cache, // Pass the populated cache
                folder_icon,
                file_icon,
                size_icon,
                date_icon,
                remote_icon,
                &service_sizes, // Pass pre-calculated service totals
            );
            let tree_output_file_path = output_dir.join(tree_base_filename);
            fs::write(&tree_output_file_path, &tree_report_string)?;
            println!(
                "Tree report (single file) written to '{}'",
                tree_output_file_path.display()
            );
        }
        OutputMode::Remotes => {
            // Generate one file per service.
            let mut sorted_services: Vec<String> = files_data.get_service_names(); // Get sorted service names
            sorted_services.sort(); // Ensure sorted order
            for service_name in sorted_services {
                // Generate the tree string specifically for this service using the cache.
                let (service_string, _service_calculated_size) = files_data
                    .generate_service_tree_string(
                        &service_name,
                        &size_cache, // Pass the cache
                        folder_icon,
                        file_icon,
                        size_icon,
                        date_icon,
                        remote_icon,
                    );
                // Construct the filename (e.g., "_RemoteName files.txt")
                let service_filename =
                    format!("{MODE_REMOTES_SERVICE_PREFIX}{service_name} {tree_base_filename}"); // Note: Changed format slightly
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
            // Generate a directory structure mirroring the remotes.
            let mut sorted_services: Vec<String> = files_data.get_service_names();
            sorted_services.sort(); // Ensure sorted order
            println!("Writing folder structure to '{}'...", output_dir.display());
            for service_name in sorted_services {
                // Create the base directory for the service.
                let service_dir_path = output_dir.join(&service_name);
                // fs::create_dir_all(&service_dir_path)?; // Ensure service directory exists - handled inside recursive call now? No, need it here.

                // Get root keys for this service.
                let root_keys = files_data
                    .roots_by_service
                    .get(&service_name)
                    .cloned()
                    .unwrap_or_default();

                if root_keys.is_empty() {
                    println!("  - No root items found for remote '{}', skipping folder structure generation.", service_name);
                    continue; // Skip if no roots (e.g., empty remote)
                }

                // Need to sort roots here too: Dirs first, then alpha
                let mut sorted_root_keys = root_keys;
                sorted_root_keys.sort_by(|a_key, b_key| {
                    match (files_data.files.get(a_key), files_data.files.get(b_key)) {
                        (Some(a), Some(b)) => a
                            .is_dir
                            .cmp(&b.is_dir) // Dirs first
                            .reverse() // Reverse bool comparison
                            .then_with(|| a.get_display_name().cmp(&b.get_display_name())), // Then alpha
                        _ => Ordering::Equal, // Simplified error handling for missing keys
                    }
                });

                println!(
                    "  - Processing remote '{}' (output root: '{}')...",
                    service_name,
                    service_dir_path.display()
                );
                // Iterate through sorted root items for this service.
                for root_key in &sorted_root_keys {
                    if let Some(root_file) = files_data.files.get(root_key) {
                        // Only recursively process directories at the root level.
                        // Files at the root won't be written as individual files,
                        // they would need to be listed in a root-level content file if desired (not implemented).
                        if root_file.is_dir {
                            // Start recursive writing process for this root directory.
                            // The function will create subdirs and content files.
                            root_file.write_fs_node_recursive(
                                &service_dir_path, // Base path for this service
                                &files_data.files,
                                &size_cache, // Pass the cache
                                folder_icon,
                                file_icon,
                                size_icon,
                                date_icon,
                                folder_content_filename, // Name for content file (e.g., files.txt)
                            )?;
                        } else {
                            // Optionally handle files at the root of the service here.
                            // Could create a root-level content file, or just skip them.
                            // Current logic: Files at the root are ignored in Folders mode.
                        }
                    } else {
                        // Log if a root key is somehow not found.
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

    // --- 6. Generate HTML Treemap Report (if enabled) ---
    let treemap_output_path = output_dir.join(treemap_output_filename);
    if enable_html_treemap {
        println!("Generating HTML treemap visualization...");
        // Generate the hierarchical data structure needed by ECharts using the cache.
        match files_data.generate_echarts_data(&size_cache, &service_sizes) {
            // If data generation is successful:
            echarts_data => {
                // Generate the full HTML content embedding the data.
                match generate_echarts_html(&echarts_data, "Cloud Storage Treemap") {
                    // Pass title for HTML tag
                    Ok(html_content) => {
                        // Write the HTML content to the output file.
                        if let Err(e) = fs::write(&treemap_output_path, html_content) {
                            // Log error if writing fails.
                            eprintln!(
                                "Error writing HTML treemap report to '{}': {}",
                                treemap_output_path.display(),
                                e
                            );
                            // Consider returning an error here if this report is critical.
                        } else {
                            // Log success message.
                            println!(
                                "HTML treemap report written to '{}'",
                                treemap_output_path.display()
                            );
                        }
                    }
                    Err(e) => {
                        // Log error if HTML generation itself fails (e.g., JSON serialization error).
                        eprintln!("Error generating ECharts HTML content: {}", e);
                    }
                }
            } // Note: generate_echarts_data itself currently doesn't return errors, only logs warnings.
        }
    } else {
        println!("HTML treemap visualization skipped.");
        // If disabled, remove any pre-existing treemap file.
        if treemap_output_path.exists() {
            let _ = fs::remove_file(&treemap_output_path);
        }
    }

    println!("Reports generation finished.");
    Ok(()) // Indicate overall success of report generation phase.
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
    results: Vec<AboutResult>, // Takes the collected results from parallel execution
    about_output_path: &str,   // Path for the output file
) -> Result<(), Box<dyn std::error::Error>> {
    if results.is_empty() {
        println!("No 'about' results to process.");
        // Write an empty or placeholder report if desired, or just return.
        fs::write(about_output_path, "No remote information available.")?;
        return Ok(());
    }
    println!("Processing 'about' results...");

    // Store results temporarily as (name, formatted_line) for easy sorting later.
    let mut report_lines: Vec<(String, String)> = Vec::new();
    // Variables to accumulate grand totals across successful remotes.
    let mut grand_total_used: u64 = 0;
    let mut grand_total_free: u64 = 0;
    let mut grand_total_trashed: u64 = 0;
    let mut remotes_with_data = 0; // Count remotes that returned valid data.
    let mut errors_encountered = 0; // Count remotes that resulted in an error.

    // Process results sequentially after parallel fetch
    for result in results {
        match result {
            Ok((remote_name, about_info)) => {
                // Successfully fetched and parsed info for this remote.
                remotes_with_data += 1;

                // Accumulate totals (use unwrap_or(0) to handle Option<u64>)
                grand_total_used += about_info.used.unwrap_or(0);
                grand_total_free += about_info.free.unwrap_or(0);
                grand_total_trashed += about_info.trashed.unwrap_or(0);

                // Format the line for this remote.
                // Use human_bytes for readability, handle Option::None with "N/A".
                let used_str = about_info
                    .used
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                let total_str = about_info
                    .total
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                let free_str = about_info
                    .free
                    .map_or_else(|| "N/A".to_string(), |v| human_bytes(v as f64));
                // Only include Trashed/Other if they are present and non-zero.
                let trashed_str = about_info.trashed.filter(|&v| v > 0).map_or_else(
                    || "".to_string(),                                  // Empty string if None or 0
                    |v| format!(", Trashed={}", human_bytes(v as f64)), // Formatted string if present
                );
                let other_str = about_info.other.filter(|&v| v > 0).map_or_else(
                    || "".to_string(),                                // Empty string if None or 0
                    |v| format!(", Other={}", human_bytes(v as f64)), // Formatted string if present
                );

                // Construct the final line for this remote.
                let line = format!(
                    "{}: Used={}, Free={}, Total={}{}{}",
                    remote_name, used_str, free_str, total_str, trashed_str, other_str
                );
                report_lines.push((remote_name.clone(), line)); // Store name for sorting, and the line
            }
            Err((remote_name, err_msg)) => {
                // An error occurred fetching or parsing info for this remote.
                errors_encountered += 1;
                let line = format!("{}: Error - {}", remote_name, err_msg); // Format error line
                                                                            // Log the specific error message to stderr.
                                                                            // Note: The error might have already been logged by run_command if it was an execution failure.
                eprintln!(
                    "  Error processing 'about' for {}: {}",
                    remote_name, err_msg
                );
                report_lines.push((remote_name.clone(), line)); // Store name and error line
            }
        }
    }

    // Sort the collected lines alphabetically by remote name.
    report_lines.sort_by(|a, b| a.0.cmp(&b.0));

    // Prepare the final list of strings for the report file content.
    let mut final_report_lines: Vec<String> = Vec::new();
    // Add the sorted lines (discarding the remote name used for sorting).
    final_report_lines.extend(report_lines.into_iter().map(|(_, line)| line));

    // Add the Grand Total summary line if any data was successfully collected.
    if remotes_with_data > 0 {
        final_report_lines.push("".to_string()); // Blank line separator
                                                 // Calculate an effective total capacity (Used + Free) if both are available.
        let effective_total_capacity = grand_total_used + grand_total_free;
        let grand_total_line = format!(
            "Grand Total ({} remotes reporting data): Used={}, Free={}, Trashed={}, Total Capacity={}",
            remotes_with_data,
            human_bytes(grand_total_used as f64),
            human_bytes(grand_total_free as f64),
            human_bytes(grand_total_trashed as f64),
             // Display calculated capacity or "N/A" if Used+Free is 0 (or data was missing)
            if effective_total_capacity > 0 { human_bytes(effective_total_capacity as f64) } else { "N/A".to_string() },
        );
        final_report_lines.push(grand_total_line);
    } else if errors_encountered > 0 {
        // If only errors occurred, indicate no data available.
        final_report_lines.push("".to_string());
        final_report_lines.push("Grand Total: No size data available due to errors.".to_string());
    } else {
        // If no data and no errors (e.g., no remotes queried), indicate no data.
        final_report_lines.push("".to_string());
        final_report_lines.push("Grand Total: No size data available from any remote.".to_string());
    }

    // Add a note if errors were encountered during fetching.
    if errors_encountered > 0 {
        final_report_lines.push(format!(
            "\nNote: Encountered errors fetching 'about' info for {} remote(s). See logs for details.",
            errors_encountered
        ));
    }

    // Join all lines and write the final report string to the specified file path.
    let final_report_string = final_report_lines.join("\n");
    fs::write(about_output_path, final_report_string)?; // Propagate potential file write error
    println!("About report written to '{}'", about_output_path);

    // Currently returns Ok(()) even if individual remote fetches failed (errors are logged).
    // Consider returning an error if `errors_encountered > 0` if that behavior is desired.
    if errors_encountered > 0 {
        Ok(()) // FIXME Or return an error? return Ok for now, errors are logged
    } else {
        Ok(())
    }
}
