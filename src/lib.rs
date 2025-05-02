use clap::Parser;
use human_bytes::human_bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::{collections::HashMap, fs};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = ("./".to_string()))]
    path: String,

    #[arg(short, long, default_value_t = false)]
    duplicates: bool,
}

#[derive(Serialize, Deserialize, Eq, Debug, Hash, Clone, Ord, PartialOrd)]
struct Hashes {
    sha1: Option<String>,
    dropbox: Option<String>,
    md5: Option<String>,
    sha256: Option<String>,
    quickxor: Option<String>,
}

impl PartialEq for Hashes {
    fn eq(&self, other: &Self) -> bool {
        let sha1_equal = match &self.sha1 {
            Some(hs) => match &other.sha1 {
                Some(ho) => hs == ho,
                None => false,
            },
            None => false,
        };
        if sha1_equal {
            return true;
        }

        let dropbox_equal = match &self.dropbox {
            Some(hs) => match &other.dropbox {
                Some(ho) => hs == ho,
                None => false,
            },
            None => false,
        };
        if dropbox_equal {
            return true;
        }

        let md5_equal = match &self.md5 {
            Some(hs) => match &other.md5 {
                Some(ho) => hs == ho,
                None => false,
            },
            None => false,
        };
        if md5_equal {
            return true;
        }

        let sha256_equal = match &self.sha256 {
            Some(hs) => match &other.sha256 {
                Some(ho) => hs == ho,
                None => false,
            },
            None => false,
        };
        if sha256_equal {
            return true;
        }

        let quickxor_equal = match &self.quickxor {
            Some(hs) => match &other.quickxor {
                Some(ho) => hs == ho,
                None => false,
            },
            None => false,
        };

        quickxor_equal
    }
}

#[derive(Serialize, Deserialize, Eq)]
struct RawFile {
    Path: String,
    Name: String,
    Size: i64,
    MimeType: String,
    ModTime: String,
    IsDir: bool,
    // ID: String,
    Hashes: Option<Hashes>,
}

impl PartialEq for RawFile {
    fn eq(&self, other: &Self) -> bool {
        self.Path == other.Path
            && self.ModTime == other.ModTime
            && self.Size == other.Size
            && self.IsDir == other.IsDir
            && self.MimeType == other.MimeType
            // && self.ID == other.ID
            && self.Name == other.Name
    }
}

impl PartialOrd for RawFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.IsDir && !other.IsDir {
            return std::cmp::Ordering::Less;
        }
        if !self.IsDir && other.IsDir {
            return std::cmp::Ordering::Greater;
        }
        if self == other {
            return std::cmp::Ordering::Equal;
        }
        let ns = self.Path.split("/").count();
        let no = other.Path.split("/").count();
        if ns < no {
            return std::cmp::Ordering::Less;
        }
        if ns > no {
            return std::cmp::Ordering::Greater;
        }
        self.Path.cmp(&other.Path)
    }
}

fn get_name_and_extension(path: &str) -> (String, Option<String>) {
    match path.rfind('.') {
        Some(idx) => {
            if idx == 0 {
                return (path.to_string(), None);
            }
            let name = &path[..idx];
            let ext = &path[idx + 1..];
            (name.to_string(), Some(ext.to_string()))
        }
        None => (path.to_string(), None),
    }
}

#[derive(Debug, Clone)]
struct File {
    service: String,
    ext: String,
    path: Vec<String>,
    modified: String,
    size: usize,
    children_keys: HashSet<String>,
}

impl File {
    fn from(service: String, raw_file: &RawFile) -> Self {
        let mut path = raw_file
            .Path
            .split("/")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        // let modified = raw_file.ModTime.clone();

        let ext = match raw_file.IsDir {
            true => "".to_string(),
            false => {
                let (name, ext) = get_name_and_extension(&raw_file.Name);
                match ext {
                    Some(x) => {
                        let p = path.last_mut().unwrap();
                        *p = name;
                        x
                    }
                    None => "".to_string(),
                }
            }
        };
        let size: usize = match raw_file.Size {
            -1 | 0 => 0,
            s => s as usize,
        };
        Self {
            service,
            ext,
            path: path,
            modified: raw_file.ModTime.clone(),
            size,
            children_keys: HashSet::new(),
        }
    }
    fn new(service: String, raw_path: String, modified: String, raw_size: i64) -> Self {
        // let mut full_path = vec![service];
        let mut path: Vec<String> = raw_path.split("/").map(|s| s.to_string()).collect();
        // full_path.append(&mut path);

        let (name, ext) = get_name_and_extension(&path.last().unwrap());

        let ext = match ext {
            Some(x) => {
                let n = path.len() - 1;
                path[n] = name;
                x
            }
            None => "".to_string(),
        };

        let size: usize = match raw_size {
            -1 | 0 => 0,
            _ => raw_size as usize,
        };
        Self {
            service,
            ext,
            path: path,
            modified,
            size,
            children_keys: HashSet::new(),
        }
    }

    fn get_key(&self) -> String {
        // let level = self.path.len() - 1;
        // let name = self.path.get(level).unwrap();
        format!(
            "{}:{}-{}[{}]",
            self.service,
            self.path.join("-"),
            self.ext,
            self.size
        )
    }

    fn get_parent_key(&self) -> Option<String> {
        match self.path.len() {
            0 | 1 => None,
            len => {
                // let level = len - 2;
                let path: Vec<String> = self
                    .path
                    .iter()
                    .take(len - 1)
                    .map(|x| x.to_string())
                    .collect();
                Some(format!("{}:{}-[0]", self.service, path.join("-")))
            }
        }
    }

    fn parse(&self, spaces: usize, files: &HashMap<String, File>) -> (String, usize) {
        let name = self.path.last().unwrap();
        let indent = " ".repeat(spaces * (self.path.len() - 1));
        let init_indent = " ".repeat(spaces);
        let mut dot_ext: String = "".to_string();
        let modified = self.modified.clone();
        let mut starter = "";
        if self.ext.is_empty() {
            starter = "📁";
        } else {
            dot_ext = format!(".{}", self.ext);
        }

        let mut num_size = self.size;

        let mut children_strings: Vec<String> = Vec::new();
        let mut sep = "";
        if self.children_keys.len() > 0 {
            sep = "\n";
            for key in &self.children_keys {
                let (child_string, child_size) = files.get(key).unwrap().parse(spaces, files);
                num_size += child_size;
                children_strings.push(child_string);
            }
        }
        let size = human_bytes(num_size as f64);
        let c = children_strings.join("\n");
        // let k = self.get_key();
        let s =
            format!("{init_indent}{indent}{starter}{name}{dot_ext} 💾{size} 📅{modified}{sep}{c}");
        (s, num_size)
    }
}

struct Duplicates {
    paths: Vec<String>,
    size: usize,
}

struct Files {
    files: HashMap<String, File>,
    duplicates: BTreeMap<Hashes, Duplicates>,
}

impl Files {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
            duplicates: BTreeMap::new(),
        }
    }

    fn add_file(&mut self, file: File) {
        let key = file.get_key();
        let file = file.clone();
        match self.files.insert(key.clone(), file.clone()) {
            Some(old_file) => {
                println!("key already exists: '{key}'");
                println!("old file: {old_file:?}");
                println!("new file: {file:?}\n");
            }
            None => {}
        };
    }

    fn add_duplicate(&mut self, path1: &str, path2: &str, hashes: &Hashes, size: usize) {
        if self.duplicates.contains_key(hashes) {
            let dups = self.duplicates.get_mut(hashes).unwrap();
            dups.paths.push(path1.to_string());
            dups.paths.push(path2.to_string());
            dups.size = size;
        } else {
            self.duplicates.insert(
                hashes.clone(),
                Duplicates {
                    paths: vec![path1.to_string(), path2.to_string()],
                    size: size,
                },
            );
        }
    }
}

fn main() {
    let args = Args::parse();
    println!("ARGS: {:?}", args);
    let paths = fs::read_dir(args.path).unwrap();
    let mut raw_files: HashMap<String, Vec<RawFile>> = HashMap::new();
    for path in paths {
        let path = path.unwrap().path();
        print!("FILE: {}", &path.display());
        if !(path.starts_with(".") && !path.starts_with("./"))
            && path.extension().is_some()
            && path.extension().unwrap().to_str().unwrap().eq("json")
        {
            println!(" -> V");
        } else {
            println!(" -> X");
            continue;
        }
        let contents = fs::read_to_string(&path).expect("Should have been able to read the file");
        let service = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .replace("_AT_", "@")
            .replace("_DOT_", ".")
            .replace("_", " ");
        let service = format!("🔴 {service}");
        let mut local_raw_files: Vec<RawFile> = match serde_json::from_str(&contents) {
            Ok(x) => x,
            Err(_) => {
                println!("JSON ERROR: {}", path.display());
                continue;
            }
        };
        local_raw_files.sort();
        raw_files.insert(service, local_raw_files);
    }

    let mut fs = Files::new();
    for index_service_1 in 0..raw_files.len() {
        let key_service_1 = raw_files.keys().nth(index_service_1).unwrap();
        println!("SERVICE: {}", key_service_1);
        let files_service_1 = &raw_files[key_service_1];
        for i in 0..files_service_1.len() {
            let raw_file = &files_service_1[i];
            let file = File::from(key_service_1.clone(), raw_file);
            fs.add_file(file);

            if args.duplicates {
                // test for duplicated
                let h1 = match &raw_file.Hashes {
                    Some(h) => h,
                    None => continue,
                };
                for j in (i + 1)..files_service_1.len() {
                    let h2 = match &files_service_1[j].Hashes {
                        Some(h) => h,
                        None => continue,
                    };
                    if h1 == h2 {
                        let d1 = format!(
                            "{key_service_1}:{} [{}]",
                            &files_service_1[i].Path,
                            human_bytes(files_service_1[i].Size as f64)
                        );
                        let d2 = format!(
                            "{key_service_1}:{} [{}]",
                            &files_service_1[j].Path,
                            human_bytes(files_service_1[j].Size as f64)
                        );
                        fs.add_duplicate(&d1, &d2, h1, files_service_1[i].Size as usize);
                    }
                }
                for index_service_2 in (index_service_1 + 1)..raw_files.len() {
                    let key_service_2 = raw_files.keys().nth(index_service_2).unwrap();
                    let files_service_2 = &raw_files[key_service_2];
                    for jv in 0..files_service_2.len() {
                        let h2 = match &raw_files[key_service_2][jv].Hashes {
                            Some(h) => h,
                            None => continue,
                        };
                        if h1 == h2 {
                            let d1 = format!(
                                "{key_service_1}:{} [{}]",
                                &raw_files[key_service_1][i].Path,
                                human_bytes(raw_files[key_service_1][i].Size as f64)
                            );
                            let d2 = format!(
                                "{key_service_2}:{} [{}]",
                                &files_service_2[jv].Path,
                                human_bytes(files_service_2[jv].Size as f64)
                            );
                            fs.add_duplicate(&d1, &d2, h1, raw_files[key_service_1][i].Size as usize);
                        }
                    }
                }
            }
        }
    }
    if args.duplicates {
        let mut duplicates = Vec::from_iter(fs.duplicates);
        duplicates.sort_by(|a, b| {
            let sa = a.1.paths.len() * a.1.size;
            let sb = b.1.paths.len() * b.1.size;
            sb.cmp(&sa)
        });
        let mut lines: Vec<String> = Vec::new();
        for (hashes, dups) in &duplicates {
            lines.push(format!("🔴{}", human_bytes(dups.size as f64)));
            for path in &dups.paths {
                lines.push(path.to_string());
            }
            match &hashes.md5 {
                Some(x) => lines.push(format!("MD5: {x:?}")),
                None => {}
            };
            match &hashes.sha1 {
                Some(x) => lines.push(format!("SHA1: {x:?}")),
                None => {}
            };
            match &hashes.sha256 {
                Some(x) => lines.push(format!("SHA256: {x:?}")),
                None => {}
            };
            match &hashes.dropbox {
                Some(x) => lines.push(format!("DROPBOX: {x:?}")),
                None => {}
            };
            match &hashes.quickxor {
                Some(x) => lines.push(format!("QUICKXOR: {x:?}")),
                None => {}
            };
            lines.push("\n".to_string());
        }
        let data = lines.join("\n");
        fs::write("duplicates.txt", data).expect("Unable to write file");
    }

    println!("TREE:");
    let mut roots: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut parent_to_children_keys: HashMap<String, Vec<String>> = HashMap::new();

    for file in fs.files.values_mut() {
        match file.get_parent_key() {
            Some(parent_key) => match parent_to_children_keys.get_mut(&parent_key) {
                Some(children_keys) => {
                    children_keys.push(file.get_key());
                }
                None => {
                    parent_to_children_keys.insert(parent_key, vec![file.get_key()]);
                }
            },
            None => match roots.get_mut(&file.service) {
                Some(keys) => {
                    keys.push(file.get_key());
                }
                None => {
                    roots.insert(file.service.clone(), vec![file.get_key()]);
                }
            },
        }
    }
    for (key, children_keys) in parent_to_children_keys {
        let file = match fs.files.get_mut(&key) {
            Some(file) => file,
            None => {
                println!("key not found: {}", key);
                continue;
            }
        };
        file.children_keys = children_keys.into_iter().collect();
    }
    let mut total_size = 0;
    let mut final_text: Vec<String> = Vec::new();
    for (service, root_files_keys) in &roots {
        let mut total_service_size = 0;
        let mut service_text: Vec<String> = Vec::new();
        for root_index in root_files_keys {
            let (data, size) = fs.files[root_index].parse(2, &fs.files);
            total_service_size += size;
            service_text.push(data);
        }
        let service_header = format!("{}: {}", service, human_bytes(total_service_size as f64));
        service_text.insert(0, service_header);
        final_text.append(&mut service_text);
        total_size += total_service_size;
    }
    let str_size = human_bytes(total_size as f64);
    let header = format!("Total cloud size used: {str_size}");
    final_text.insert(0, header.clone());
    let data = final_text.join("\n");

    fs::write("files.txt", data).expect("Unable to write file");
    fs::write("size used.txt", header).expect("Unable to write file");
}