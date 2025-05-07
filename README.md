# CloudMapper: Open-source tool to map and visualize your cloud storage landscape.

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/cloudmapper.svg)](https://crates.io/crates/cloudmapper)
[![Repository](https://img.shields.io/badge/GitHub-tesserato/CloudMapper-blue?logo=github)](https://github.com/tesserato/CloudMapper)
[![Downloads](https://img.shields.io/github/downloads/tesserato/CloudMapper/total.svg)](https://github.com/tesserato/CloudMapper/releases)

##  WinDirStat for the cloud - Understand your scattered cloud storage at a glance

> [!IMPORTANT] 
> CloudMapper operates in a **read-only** manner regarding your cloud storage. It **does not have direct access** to your cloud storage providers. All interactions with your cloud services are performed by invoking the `rclone` command-line tool. CloudMapper **cannot write to, delete from, or modify your cloud storage in any way**. Its operations are limited to listing files/folders and querying storage usage information via `rclone`. Any report suggesting potential savings (e.g., from duplicates) is for informational purposes only; actions to modify cloud data must be taken by you, typically using `rclone` or your cloud provider's interface directly.


CloudMapper is a command-line utility designed to help you understand and Analyse your cloud storage. It uses [rclone](https://rclone.org) to interface with various cloud storage providers, gathers information about your files and their structure, and then generates several insightful reports, including:

*   A detailed text tree view of your files and folders (for `Single`/`Remotes` modes) or a mirrored local directory structure with placeholders for the actual files (for `Folders` mode).
*   A report on duplicate files (based on hashes).
*   A summary of file extensions and their storage consumption.
*   A size usage report per remote and overall.
*   A report listing the N largest files found across all remotes.
*   An interactive HTML treemap visualization of your storage.
*   Simple installation (`cargo install cloudmapper`) or see [Installation](#installation) for more options.

## Example Output

**`treemap.html`: (For an similar, interactive example, click [here](https://echarts.apache.org/examples/en/editor.html?c=treemap-disk))**

![Treemap](https://raw.githubusercontent.com/tesserato/CloudMapper/refs/heads/main/treemap.jpeg)


**`files.txt` (In `Single` mode, or the content file like `_MyRemote files.txt` in `Remotes` mode, or the content file within each directory in `Folders` mode):**

```text
☁️ my-gdrive: 15.21 GiB
  📁 Documents 💽 5.1 GiB 📆 2023-10-26T10:00:00Z
    📄 report.docx 💽 1.2 MiB 📆 2023-10-25T09:00:00Z
    📁 Old Reports 💽 4.8 GiB 📆 2023-01-10T15:30:00Z
      📄 old_report_2022.pdf 💽 10.5 MiB 📆 2023-01-09T11:00:00Z
      ...
  📁 Photos 💽 10.11 GiB 📆 2023-10-27T11:00:00Z
    📄 IMG_0001.JPG 💽 4.5 MiB 📆 2023-09-01T14:00:00Z
    ...
```

**`duplicates.txt`:**

```text
Total size occupied by all files identified as duplicates: 2.5 GiB
Found 5 sets of files with matching hashes.
Total potential disk space saving by removing duplicates (keeping one copy of each): 1.5 GiB

Duplicates found with size: 1 GiB (2 files, potential saving: 1 GiB)
  - my-gdrive:backups/archive.zip
  - my-s3:important-backups/archive.zip
  Matching Hashes: SHA-1: abc..., MD5: def...

Duplicates found with size: 500 MiB (3 files, potential saving: 1 GiB)
  - my-dropbox:shared/project_data.dat
  - my-gdrive:project_x/data/project_data.dat
  - my-onedrive:archive/project_data.dat
  Matching Hashes: SHA-256: 123..., QuickXorHash: 456...
...
```

**`extensions.txt`:**

```text
Total Files Found: 12345 (Total Size: 55.8 GiB)
-------------------------------------
Extension       |      Total Size |   File Count |  % Size |  % Count
--------------------------------------------------------------------
.mkv            |       25.2 GiB |          150 |  45.16% |   1.22%
.zip            |       10.1 GiB |         1200 |  18.10% |   9.72%
.jpg            |        8.5 GiB |         8500 |  15.23% |  68.85%
.pdf            |        2.0 GiB |          500 |   3.58% |   4.05%
[no extension]  |      500.0 MiB |           25 |   0.88% |   0.20%
...
```

**`largest_files.txt`:**

```text
Top 100 Largest Files (across all remotes):
----------------------------------------------------------------------
Rank  |            Size | Path (Service:File)
----------------------------------------------------------------------
1     |       12.5 GiB | my-gdrive:videos/archive/holiday_movie_compilation.mkv
2     |        8.2 GiB | my-s3:backups-large/vm_image_backup.vmdk
3     |        5.0 GiB | my-onedrive:iso_files/linux_distro_latest.iso
...
```



## Features

*   **Comprehensive Analysis**: Leverages `rclone lsjson` and `rclone about` for detailed data.
*   **Multiple Output Modes**:
    *   `single`: A single text file for all remotes.
    *   `remotes`: One text file per remote.
    *   `folders`: A local directory structure mirroring your remotes, with placeholders for the actual files.
*   **Insightful Reports**:
    *   File/Folder tree structure with sizes and modification dates.
    *   Duplicate file detection across remotes.
    *   File extension statistics (count, total size, percentages).
    *   List of N largest files across all remotes.
    *   Overall and per-remote size usage.
    *   `rclone about` summary.
*   **Interactive Visualization**: Generates an HTML treemap using [ECharts](https://echarts.apache.org/en/index.html) for a visual overview of storage distribution.
*   **Configurable**: Control which reports are generated, rclone path, config file, and output location.
*   **Parallel Processing**: Utilizes Rayon for faster processing of multiple remotes.


## Prerequisites

*   **rclone**: `rclone` must be installed and configured with the remotes you want to Analyse. CloudMapper will attempt to use `rclone` from your system's PATH, or you can specify a path to the executable.
*   **Rust (for building from source or installing via Cargo):** Ensure you have Rust installed. You can get it from [rustup.rs](https://rustup.rs/).

## Installation

There are several ways to install CloudMapper:

### 1. Using Cargo (Recommended for Rust users)

If you have Rust and Cargo installed, you can install CloudMapper directly from [crates.io](https://crates.io/crates/cloudmapper):

```bash
cargo install cloudmapper
```
This will download the source, compile it, and install the `cloudmapper` binary in your Cargo binary directory (e.g., `~/.cargo/bin/`). Ensure this directory is in your system's PATH.

### 2. From GitHub Releases (Pre-compiled binaries)

Pre-compiled binaries for common platforms (Linux, macOS, Windows) are available on the [GitHub Releases page](https://github.com/tesserato/CloudMapper/releases).

1.  Go to the [Releases page](https://github.com/tesserato/CloudMapper/releases).
2.  Download the appropriate release for your operating system and architecture 
3.  (Optional but recommended) Move the executable to a directory in your system's PATH for easier access (e.g., `~/.local/bin/` on Linux/macOS, or a custom directory on Windows that you've added to PATH).

### 3. Building from Source

If you prefer to build from the latest source code or make modifications:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/tesserato/CloudMapper.git
    cd CloudMapper
    ```

2.  **Build the project:**
    ```bash
    cargo build --release
    ```
    The executable will be located at `target/release/cloudmapper` (or `target\release\cloudmapper.exe` on Windows).

3.  **(Optional) Add to PATH:**
    You can copy the built executable to a directory in your system's PATH.

## Usage

Once installed, you can run CloudMapper from your terminal:
```bash
cloudmapper [OPTIONS]
```

**Common Options:**

*   `-r, --rclone-path <RCLONE_PATH>`: Path to a specific rclone executable.
*   `-c, --rclone-config <RCLONE_CONFIG>`: Path to a specific rclone configuration file.
*   `-o, --output-path <OUTPUT_PATH>`: Directory for generated reports (default: `./cloud`).
*   `-m, --output-mode <OUTPUT_MODE>`: Output structure for the file list report (`single`, `remotes`, `folders`; default: `folders`).
*   `-d, --duplicates <true|false>`: Enable duplicate file report (default: `true`).
*   `-e, --extensions-report <true|false>`: Enable file extensions report (default: `true`).
*   `-a, --about-report <true|false>`: Enable 'rclone about' report (default: `true`).
*   `-l, --largest-files <COUNT>`: Number of largest files to report (default: 100, 0 to disable).
*   `-t, --html-treemap <true|false>`: Enable HTML treemap report (default: `true`).
*   `-k, --clean-output <true|false>`: Clean output directory before generating reports (default: `true`).
*   `--help`: Show help message.
*   `--version`: Show version information.

**Example:**

```bash
# Analyse all configured rclone remotes and save reports to the default './cloud' directory
cloudmapper

# Analyse remotes, save to a custom directory, and only generate the treemap and size reports
cloudmapper -o ./my_cloud_analysis --duplicates false --extensions-report false --about-report false

# Use a specific rclone binary and config, output in single file mode
cloudmapper --rclone-path /opt/rclone/rclone --rclone-config ~/.config/rclone/rclone.conf -m single
```

For detailed options, run `cloudmapper --help`.

## Reports Generated

By default, CloudMapper generates the following files in the specified output directory (e.g., `./cloud/`):

*   **File Structure Report(s):**
    *   `files.txt` (in `Single` mode, and as the content summary file within each directory in `Folders` mode. For `Folders` mode, if a service has root-level files, they will be listed in a `files.txt` within the `output_dir/<service_name>/` directory.)
    *   `_<RemoteName> files.txt` (in `Remotes` mode, one per remote)
*   **`duplicates.txt`**: Lists files with identical hashes.
*   **`extensions.txt`**: Summarizes file counts and total sizes per extension.
*   **`largest_files.txt`**: Lists the N largest files found, with their sizes and paths.
*   **`size_used.txt`**: Reports calculated total size per remote and grand total.
*   **`about.txt`**: Summarizes storage usage from `rclone about`.
*   **`treemap.html`**: An interactive HTML treemap visualization.

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues, fork the repository and send pull requests.
