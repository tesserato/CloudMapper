# CloudMapper ☁️ 🗺️

[![Build Status](https://img.shields.io/github/actions/workflow/status/YOUR_USERNAME/cloudmapper/rust.yml?branch=main)](https://github.com/YOUR_USERNAME/cloudmapper/actions) <!-- TODO: Replace YOUR_USERNAME -->
[![Crates.io](https://img.shields.io/crates/v/cloudmapper.svg)](https://crates.io/crates/cloudmapper) <!-- TODO: Update if published -->
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](https://opensource.org/licenses/MIT)

CloudMapper is a command-line tool that leverages `rclone` to analyze and visualize the contents and usage of your configured cloud storage remotes. It generates various reports to help you understand file distribution, identify duplicates, track usage by extension, and explore your storage hierarchically.

## Overview

Managing data across multiple cloud storage providers can be challenging. CloudMapper aims to simplify this by:

1.  Connecting to your existing `rclone` configuration.
2.  Fetching detailed file listings (`lsjson`) and storage usage (`about`) information, often in parallel for speed.
3.  Processing this data to build an internal representation of your cloud storage.
4.  Generating multiple human-readable and machine-parseable reports, including an interactive HTML treemap visualization.

It's designed for users who rely on `rclone` and need a better overview of their storage landscape across different services.

## Features

*   **Rclone Integration:** Works directly with your configured `rclone` remotes.
*   **Parallel Processing:** Fetches `rclone lsjson` and `rclone about` data in parallel across remotes for faster analysis.
*   **Multiple Output Modes:**
    *   `Single`: A single text file showing the combined tree structure of all remotes.
    *   `Remotes`: One text file per remote, showing its individual tree structure.
    *   `Folders`: Creates a local directory structure mirroring your remotes, with `files.txt` in each directory listing its contents (Default).
*   **Comprehensive Reports:**
    *   **File Tree/Listing (`files.txt` or Folder Structure):** Hierarchical view with icons (📁/📄), calculated sizes (💽), and modification dates (📆).
    *   **Size Usage (`size_used.txt`):** Summary of total calculated size per remote and the grand total based on file listings.
    *   **Rclone About Summary (`about.txt`):** Aggregated output from `rclone about --json` for each remote, showing total, used, free, and trashed space reported by the provider.
    *   **Duplicate File Report (`duplicates.txt`):** Identifies potential duplicate files across all scanned remotes based on available file hashes (MD5, SHA-1, SHA-256, DropboxHash, QuickXorHash), sorted by potential space savings.
    *   **Extensions Report (`extensions.txt`):** Table summarizing file counts and total size per file extension, sorted by total size.
    *   **HTML Treemap (`treemap.html`):** Interactive treemap visualization (using ECharts) showing storage usage hierarchically. Allows drilling down into folders, hovering for details, and panning/zooming.

*   **Configuration:** Control behavior via command-line arguments or environment variables.
*   **Optimized Release Builds:** Configured for performance (`LTO`, `codegen-units=1`, etc.).

## Example Output Snippets

**(Note: Actual appearance may vary based on terminal support for icons/emoji.)**

**`files.txt` (Single/Remotes Mode) or content within Folders Mode:**

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

**`treemap.html`:**
An interactive HTML page showing nested rectangles where the area represents the size. You can click rectangles (folders) to zoom in, hover to see the full path and size, and use a breadcrumb trail to navigate back up. Uses a dark theme by default.

## Prerequisites

1.  **Rclone:** CloudMapper *requires* `rclone` to be installed and configured on your system. You need to have your cloud remotes set up using `rclone config` *before* running CloudMapper. Visit the [rclone website](https://rclone.org/) for installation instructions.
2.  **Rust:** If building from source, you need the Rust toolchain (including `cargo`) installed. Visit [rustup.rs](https://rustup.rs/) to install it.

## Installation

### Option 1: From Crates.io (Recommended - If Published)

```bash
cargo install cloudmapper
```
<!-- TODO: Uncomment and ensure package is published before claiming this -->

### Option 2: From Source

1.  Clone the repository:
    ```bash
    git clone https://github.com/YOUR_USERNAME/cloudmapper.git # TODO: Replace YOUR_USERNAME
    cd cloudmapper
    ```
2.  Build the release binary:
    ```bash
    cargo build --release
    ```
3.  The executable will be located at `target/release/cloudmapper`. You can copy this binary to a location in your system's PATH (e.g., `~/.local/bin/` or `/usr/local/bin/`).

## Usage

The basic command structure is:

```bash
cloudmapper [OPTIONS]
```

**Common Options:**

*   `-o, --output-path <PATH>`: **Required (or use `CM_OUTPUT` env var)**. Specifies the directory where reports will be saved (Default: `./cloud`).
*   `-m, --output-mode <MODE>`: How to structure the file list output (`single`, `remotes`, `folders`). Default: `folders`. (Env: `CM_OUTPUT_MODE`)
*   `-r, --rclone-path <PATH>`: Path to the `rclone` executable if not in PATH. (Env: `RCLONE_EXECUTABLE`)
*   `-c, --rclone-config <PATH>`: Path to a specific `rclone.conf` file. (Env: `RCLONE_CONFIG`)
*   `-k, --clean-output <true|false>`: Clean (remove) the output directory before running. Default: `true`. (Env: `CM_CLEAN_OUTPUT`)
*   `-d, --duplicates <true|false>`: Enable/disable duplicates report. Default: `true`. (Env: `CM_DUPLICATES`)
*   `-e, --extensions-report <true|false>`: Enable/disable extensions report. Default: `true`. (Env: `CM_EXTENSIONS`)
*   `-a, --about-report <true|false>`: Enable/disable `rclone about` summary report. Default: `true`. (Env: `CM_ABOUT`)
*   `-t, --html-treemap <true|false>`: Enable/disable HTML treemap report. Default: `true`. (Env: `CM_HTML_TREEMAP`)
*   `--help`: Show all available options and their descriptions.

**Example:**

```bash
# Run with default settings, outputting to ./my-cloud-reports
cloudmapper -o ./my-cloud-reports

# Run using a specific rclone config, disable duplicates, output as single file
cloudmapper -c ~/.config/rclone/rclone.conf -o ./reports --output-mode single --duplicates false

# Run using environment variables
export CM_OUTPUT="./reports"
export CM_OUTPUT_MODE="remotes"
export CM_DUPLICATES="false"
cloudmapper
```

## Configuration via Environment Variables

All command-line options can alternatively be set using environment variables:

| Environment Variable  | Corresponding Flag     | Description                                       |
| :-------------------- | :--------------------- | :------------------------------------------------ |
| `RCLONE_EXECUTABLE`   | `-r`, `--rclone-path`  | Path to `rclone` executable                       |
| `RCLONE_CONFIG`       | `-c`, `--rclone-config` | Path to `rclone.conf` file                        |
| `CM_OUTPUT`           | `-o`, `--output-path`  | Output directory path                             |
| `CM_OUTPUT_MODE`      | `-m`, `--output-mode`  | Output mode (`single`, `remotes`, `folders`)      |
| `CM_DUPLICATES`       | `-d`, `--duplicates`   | Enable duplicates report (`true` or `false`)      |
| `CM_EXTENSIONS`       | `-e`, `--extensions-report` | Enable extensions report (`true` or `false`) |
| `CM_ABOUT`            | `-a`, `--about-report` | Enable `about` report (`true` or `false`)         |
| `CM_HTML_TREEMAP`     | `-t`, `--html-treemap` | Enable HTML treemap report (`true` or `false`)    |
| `CM_CLEAN_OUTPUT`     | `-k`, `--clean-output` | Clean output directory (`true` or `false`)        |

*Note: Command-line arguments take precedence over environment variables.*

## Output Files Description

CloudMapper generates the following files in the specified output directory (by default):

*   **`files.txt`** (In `Single` or `Remotes` mode) OR **Directory Structure** (In `Folders` mode):
    *   **Single/Remotes:** A text file containing the hierarchical file listing.
    *   **Folders:** A directory structure matching your remotes. Inside each created directory, a `files.txt` lists the contents (files and subdirectories) of that specific remote directory.
*   **`size_used.txt`**: Text file summarizing the total calculated size (from `lsjson`) for each remote and the overall total.
*   **`about.txt`**: Text file summarizing the output of `rclone about --json` for each remote, showing provider-reported usage stats (Total, Used, Free, Trashed).
*   **`duplicates.txt`** (If enabled): Text file listing sets of files identified as duplicates based on matching hashes, ordered by potential space saving. Includes file paths and the matching hash values.
*   **`extensions.txt`** (If enabled): Text file table showing file counts, total size, and percentage breakdown per file extension, sorted by total size descending.
*   **`treemap.html`** (If enabled): An interactive HTML file visualizing storage usage as a treemap. Open this file in your web browser.

## Development & Contributing

Contributions are welcome!

1.  **Fork** the repository on GitHub.
2.  Create a new **branch** for your feature or bugfix.
3.  Make your changes. Ensure code is formatted with `cargo fmt`.
4.  Run `cargo clippy` and address any warnings.
5.  Consider adding tests for your changes. Run tests with `cargo test`.
6.  **Commit** your changes with clear messages.
7.  Push your branch to your fork.
8.  Create a **Pull Request** against the main repository's `main` branch.

Please open an issue first to discuss significant changes.

## License

This project is licensed under either of

*   Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
*   MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

<!-- TODO: Ensure LICENSE-APACHE and LICENSE-MIT files exist in the repo -->

## Acknowledgements

*   **Rclone:** The core dependency for cloud interaction. ([rclone.org](https://rclone.org/))
*   **Clap:** For command-line argument parsing. ([crates.io/crates/clap](https://crates.io/crates/clap))
*   **Serde:** For JSON serialization/deserialization. ([crates.io/crates/serde](https://crates.io/crates/serde))
*   **Rayon:** For parallel processing. ([crates.io/crates/rayon](https://crates.io/crates/rayon))
*   **human_bytes:** For formatting sizes into human-readable units. ([crates.io/crates/human_bytes](https://crates.io/crates/human_bytes))
*   **ECharts:** For the interactive HTML treemap visualization. ([echarts.apache.org](https://echarts.apache.org/))
```