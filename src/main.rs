use std::{
    collections::{HashMap, HashSet},
    fs::create_dir_all,
    fs::File as IOFile,
    io::Read,
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use flate2::read::GzDecoder;
use futures::{stream::iter, StreamExt};
use indicatif::ProgressBar;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tar::Archive;
use tl::{parse, Bytes, Node, ParserOptions};
use tokio::{
    self,
    fs::{read_dir, File},
    io::{self, AsyncWriteExt},
};
use tree_sitter::{Language as TSLanguage, Parser as TSParser, Query, QueryCursor};

const ALL_PKGS_URL: &str = "https://sources.debian.org/api/list";
const PKG_VERSIONS_URL: &str = "https://sources.debian.org/api/src/{packagename}/";
const PKG_INFO_URL: &str = "https://sources.debian.org/api/info/package/{packagename}/{version}/";
const STATUS_URL: &str = "https://sources.debian.org/api/ping/";
const PKG_SRC_URL: &str = "https://packages.debian.org/{distro}/{platform}/{packagename}";
const CONCURRENT_REQUESTS: usize = 32;
use phf::{phf_map, Map};

const TESTING_LIMIT: isize = -1;

static LANGUAGE_EXTENSIONS: Map<&'static str, &[&str]> = phf_map! {
    "ansic" => &["c", "h"],
};

extern "C" {
    fn tree_sitter_c() -> TSLanguage;
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, ValueEnum, Debug)]
enum Language {
    /// C
    Ansic,
}

impl Language {
    fn tostr(&self) -> &'static str {
        match self {
            Language::Ansic => "ansic",
        }
    }

    fn fromstr(s: &str) -> Option<Language> {
        match s {
            "ansic" => Some(Language::Ansic),
            _ => None,
        }
    }
}

#[derive(Parser, Debug)]
struct Args {
    /// Number of packages to search
    #[clap(short, long)]
    count: usize,
    /// Directory to output pcakges to
    #[clap(short, long)]
    output: PathBuf,
    /// An optional seed to use to randomly select packages for reproducibility
    #[clap(short, long)]
    seed: Option<u64>,
    /// A language to search for (only C is currently supported)
    #[clap(short, long)]
    language: Language,
    /// An optional distro to use for package source URLs (default: bullseye)
    #[clap(short = 'D', long)]
    distro: Option<String>,
    /// An optional platform to use for package source URLs (default: amd64)
    #[clap(short, long)]
    platform: Option<String>,
    /// An optional cache file to use
    /// If a populated cache file is used, the program will use `output` as an existing directory
    #[clap(short = 'C', long)]
    cache: Option<PathBuf>,
    #[clap(short = 'S', long)]
    scan: bool,
    #[clap(num_args = 1.., last = true)]
    keywords: Vec<String>,
}

#[derive(Deserialize)]
struct PackagesResponseName {
    name: String,
}

#[derive(Deserialize)]
struct PackagesResponse {
    packages: Vec<PackagesResponseName>,
}

#[derive(Deserialize)]
struct PackageVersionsResponseVersion {
    area: String,
    version: String,
    suites: Vec<String>,
}

#[derive(Deserialize)]
struct PackageVersionsResponse {
    package: String,
    name: Option<String>,
    pathl: Vec<Vec<String>>,
    suite: String,
    #[serde(rename = "type")]
    _type: String,
    versions: Vec<PackageVersionsResponseVersion>,
}

#[derive(Deserialize)]
struct PackageInfoResponsePkgInfosMetric {
    size: usize,
}

#[derive(Deserialize)]
struct PackageInfoResponsePkgInfos {
    area: String,
    copyright: bool,
    ctags_count: usize,
    license: String,
    metric: PackageInfoResponsePkgInfosMetric,
    pts_link: String,
    sloc: Vec<(String, usize)>,
}

#[derive(Deserialize)]
struct PackageInfoResponse {
    package: String,
    version: String,
    pkg_infos: PackageInfoResponsePkgInfos,
}

#[derive(Deserialize, Serialize)]
struct Cache {
    location: Option<PathBuf>,
    seed: isize,
    packages: Vec<String>,
    package_versions: Vec<(String, String)>,
    package_infos: Vec<(String, String, HashMap<String, usize>)>,
    package_links: Vec<(String, String, String)>,
    keywords: HashMap<
        String,
        (
            Vec<(String, HashMap<String, usize>, Vec<String>, usize)>,
            Vec<(String, HashMap<String, usize>, Vec<String>, usize)>,
        ),
    >,
    results: HashMap<String, (usize, f64, f64, usize, usize)>,
}

impl Cache {
    fn new() -> Self {
        Self {
            location: None,
            seed: -1,
            packages: Vec::new(),
            package_versions: Vec::new(),
            package_infos: Vec::new(),
            package_links: Vec::new(),
            keywords: HashMap::new(),
            results: HashMap::new(),
        }
    }

    fn from(path: PathBuf) -> io::Result<Self> {
        let file = IOFile::open(path).unwrap();
        let cache = serde_json::from_reader(file)?;
        Ok(cache)
    }

    fn save(&self) -> io::Result<()> {
        if self.location.is_some() {
            let path = self.location.as_ref().unwrap();
            let file = IOFile::create(path).unwrap();
            serde_json::to_writer(file, self)?;
        } else {
            eprintln!("No cache location set, skipping save.");
        }

        Ok(())
    }
}

struct Debbie {
    jobs: usize,
    client: Client,
    rng: StdRng,
    outdir: PathBuf,
    lang: String,
    distro: String,
    platform: String,
    cache: Cache,
    language: TSLanguage,
    keywords: HashSet<String>,
}

impl Debbie {
    fn new(
        jobs: usize,
        client: Client,
        rng: StdRng,
        outdir: PathBuf,
        lang: String,
        distro: String,
        platform: String,
        cache: Cache,
        keywords: HashSet<String>,
    ) -> Self {
        let language = match Language::fromstr(&lang).unwrap() {
            Language::Ansic => unsafe { tree_sitter_c() },
        };

        Self {
            jobs,
            client,
            rng,
            outdir,
            lang,
            distro,
            platform,
            cache,
            language,
            keywords,
        }
    }

    async fn getpkgs(&mut self) -> Result<Vec<String>, reqwest::Error> {
        let resp = self.client.get(ALL_PKGS_URL).send().await?;
        let resp: PackagesResponse = resp.json().await?;
        let mut cache = &mut self.cache;

        if !cache.packages.is_empty() {
            return Ok(cache.packages.clone());
        }

        let pkgs = resp
            .packages
            .into_iter()
            .map(|pkg| pkg.name)
            // Do this later once we check if they are C
            // .choose_multiple(&mut self.rng, self.jobs);
            .collect::<Vec<_>>();

        cache.packages = pkgs.clone();

        cache.save().unwrap();

        if TESTING_LIMIT > 0 {
            Ok(pkgs
                .into_iter()
                .choose_multiple(&mut self.rng, TESTING_LIMIT as usize))
        } else {
            Ok(pkgs)
        }
    }

    async fn getversions(&mut self) -> Result<Vec<(String, String)>, reqwest::Error> {
        let pkgs = self.getpkgs().await?;
        println!("Getting versions for {} packages", pkgs.len());
        let pb = ProgressBar::new(pkgs.len() as u64);
        let cache = &mut self.cache;

        if !cache.package_versions.is_empty() {
            return Ok(cache.package_versions.clone());
        }

        let pkgs_versions = iter(pkgs)
            .map(|name| {
                let client = &self.client;
                async move {
                    let resp = client
                        .get(PKG_VERSIONS_URL.replace("{packagename}", &name))
                        .send()
                        .await?;
                    resp.json::<PackageVersionsResponse>().await
                }
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .filter_map(|versions| {
                pb.inc(1);
                let distro = &self.distro;

                async move {
                    match versions {
                        Ok(versions) => {
                            if let Some(version) = versions
                                .versions
                                .iter()
                                .filter(|v| v.suites.contains(distro))
                                .next()
                            {
                                Some((versions.package.clone(), version.version.clone()))
                            } else {
                                None
                            }
                        }
                        Err(e) => {
                            eprintln!("Error getting versions: {}", e);
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        pb.finish();

        cache.package_versions = pkgs_versions.clone();
        cache.save().unwrap();

        Ok(pkgs_versions)
    }

    async fn getinfos(
        &mut self,
    ) -> Result<Vec<(String, String, HashMap<String, usize>)>, reqwest::Error> {
        let pkgs_versions = self.getversions().await?;
        println!("Getting infos for {} packages", pkgs_versions.len());
        let pb = ProgressBar::new(pkgs_versions.len() as u64);
        let mut cache = &mut self.cache;

        if !cache.package_infos.is_empty() {
            return Ok(cache.package_infos.clone());
        }

        let pkgs_infos = iter(pkgs_versions)
            .map(|(name, version)| {
                let client = &self.client;
                async move {
                    let resp = client
                        .get(
                            PKG_INFO_URL
                                .replace("{packagename}", &name)
                                .replace("{version}", &version),
                        )
                        .send()
                        .await?;
                    resp.json::<PackageInfoResponse>().await
                }
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .filter_map(|info| {
                pb.inc(1);
                async move {
                    match info {
                        Ok(info) => {
                            let mut sloc = HashMap::new();
                            for (lang, count) in info.pkg_infos.sloc {
                                sloc.insert(lang.clone(), count);
                            }
                            Some((info.package, info.version, sloc))
                        }
                        Err(e) => {
                            eprintln!("Error getting info: {}", e);
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        pb.finish();

        cache.package_infos = pkgs_infos.clone();
        cache.save().unwrap();

        Ok(pkgs_infos)
    }

    async fn filterlangs(&mut self) -> Result<Vec<(String, String)>, reqwest::Error> {
        let pkgs_infos = self.getinfos().await.unwrap();
        println!("Filtering infos for {} packages", pkgs_infos.len());

        let lang = &self.lang;

        // Filter out non-primary lang packages
        Ok(pkgs_infos
            .into_iter()
            .filter(|(_, _, sloc)| {
                sloc.iter().max_by(|a, b| a.1.cmp(&b.1)).map(|(k, _v)| k) == Some(lang)
            })
            .map(|(name, version, _sloc)| (name, version))
            .collect::<Vec<_>>())
    }

    async fn getsrclinks(&mut self) -> Result<Vec<String>, reqwest::Error> {
        let pkgs_versions = self.filterlangs().await?;
        println!("Getting source links for {} packages", pkgs_versions.len());
        let pb = ProgressBar::new(pkgs_versions.len() as u64);
        let mut cache = &mut self.cache;

        if !cache.package_links.is_empty() {
            return Ok(cache
                .package_links
                .iter()
                .map(|(_, _, l)| l)
                .cloned()
                .collect());
        }

        let srclinks = iter(pkgs_versions)
            .map(|(name, version)| {
                let client = &self.client;
                let distro = &self.distro;
                let platform = &self.platform;

                async move {
                    let url = PKG_SRC_URL
                        .replace("{distro}", distro)
                        .replace("{platform}", platform)
                        .replace("{packagename}", &name);
                    let resp = client.get(url).send().await?;
                    resp.text().await.map(|t| (name, version, t))
                }
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .filter_map(|pagetext| {
                pb.inc(1);
                async move {
                    match pagetext {
                        Ok((name, version, pagetext)) => {
                            if let Ok(dom) = parse(&pagetext, ParserOptions::default()) {
                                let link = dom.query_selector("a[href]")?.find_map(|a| {
                                    match a.get(dom.parser()).unwrap_or(&Node::Raw(Bytes::new())) {
                                        Node::Tag(a) => {
                                            if a.attributes().contains("href") {
                                                let alink = a
                                                    .attributes()
                                                    .get("href")
                                                    .unwrap()
                                                    .unwrap()
                                                    .as_utf8_str()
                                                    .to_string();
                                                if alink.ends_with(".orig.tar.gz") {
                                                    Some((name.clone(), version.clone(), alink))
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        }
                                        _ => None,
                                    }
                                });
                                return link;
                            }
                            None
                        }
                        Err(e) => {
                            eprintln!("Error getting srclink: {}", e);
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>()
            .await;

        pb.finish();

        cache.package_links = srclinks.clone();
        cache.save().unwrap();

        Ok(srclinks.iter().map(|(_, _, l)| l.clone()).collect())
    }

    async fn picklinks(&mut self) -> Result<Vec<String>, reqwest::Error> {
        let srclinks = self.getsrclinks().await?;
        println!("Choosing {} from {} packages", self.jobs, srclinks.len());

        // Filter out non-primary lang packages
        Ok(srclinks
            .iter()
            .map(|s| s.to_string())
            .choose_multiple(&mut self.rng, self.jobs))
    }

    async fn write_file(&mut self, path: PathBuf, data: &[u8]) -> Result<(), tokio::io::Error> {
        let mut file = File::create(path).await?;
        file.write_all(data).await?;
        Ok(())
    }

    async fn download(&mut self) -> Result<(), reqwest::Error> {
        let srces = self.picklinks().await?;
        println!("Downloading {} packages", srces.len());
        let pb = ProgressBar::new(srces.len() as u64);

        let srclinks = srces
            .iter()
            .filter(|s| {
                // Check if we already have the file
                let path = PathBuf::from(&self.outdir).join(s.split('/').last().unwrap());
                !path.exists()
            })
            .collect::<Vec<_>>();

        // Iterate over srclinks in chunks of CONCURRENT_REQUESTS
        for chunk in srclinks.chunks(CONCURRENT_REQUESTS) {
            let data = iter(chunk)
                .map(|link| {
                    let client = &self.client;
                    let outdir = &self.outdir;
                    async move {
                        let resp = client.get(link.clone()).send().await?;
                        let filename = resp
                            .url()
                            .path_segments()
                            .unwrap()
                            .last()
                            .unwrap()
                            .to_string();
                        let path = outdir.join(filename);
                        resp.bytes().await.map(|data| (path, data))
                    }
                })
                .buffer_unordered(CONCURRENT_REQUESTS)
                .collect::<Vec<_>>()
                .await;
            // Now write the files
            for result in data {
                match result {
                    Ok((path, data)) => {
                        let rv = self.write_file(path, &data).await.unwrap();
                        pb.inc(1);
                        rv
                    }
                    Err(e) => {
                        eprintln!("Error downloading file: {}", e);
                    }
                }
            }
        }
        pb.finish();

        Ok(())
    }

    async fn downloadpkgs(
        &mut self,
        download_dir: PathBuf,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // If the dir doesn't exist or is empty, download the packages
        if !download_dir.is_dir() || download_dir.read_dir()?.next().is_none() {
            Ok(self.download().await?)
        // Otherwise we will use the existing files in the download dir
        } else {
            Ok(())
        }
    }

    async fn scan(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let outdir = &self.outdir;
        let mut dir = read_dir(outdir).await?;
        let extensions = LANGUAGE_EXTENSIONS.get(&self.lang).unwrap();
        let keywords = &self.keywords;
        let pb = ProgressBar::new(self.jobs as u64);

        while let Some(entry) = dir.next_entry().await? {
            if entry.metadata().await?.is_file() {
                // Iterate over the files in the tar.gz file
                let path = entry.path();
                let name = path.file_name().unwrap().to_str().unwrap().to_string();
                let file = File::open(path).await?;
                let tar = GzDecoder::new(file.into_std().await);
                let mut archive = Archive::new(tar);
                let contents: Vec<String> = archive
                    .entries()?
                    .filter_map(|e| match e {
                        Ok(mut entry) => {
                            let p = entry.path();
                            let mut res = String::new();
                            match p {
                                Ok(p) => {
                                    if let Some(ext) = p.extension() {
                                        if extensions.contains(&ext.to_str().unwrap()) {
                                            if let Ok(_) = entry.read_to_string(&mut res) {
                                                Some(res)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error reading entry: {}", e);
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error reading entry: {}", e);
                            None
                        }
                    })
                    .collect();

                let name_counts: Vec<(String, HashMap<String, usize>, Vec<String>, usize)> =
                    contents
                        .iter()
                        .filter_map(|s| {
                            let language = self.language;
                            let mut parser = TSParser::new();
                            let query = Query::new(
                                language,
                                "(function_declarator declarator: (identifier) @name ) @func",
                            )
                            .unwrap();
                            let mut querycursor = QueryCursor::new();
                            parser.set_language(language).unwrap();
                            let source = s.clone();
                            let tree = parser.parse(&s, None).unwrap();
                            let root = tree.root_node();
                            let matches =
                                querycursor.matches(&query, tree.root_node(), source.as_bytes());
                            let mut counts = HashMap::new();
                            // Add the keywords to counts with a count of 0
                            for keyword in keywords {
                                counts.insert(keyword.to_string(), 0);
                            }
                            let mut found_identifiers = Vec::new();
                            let mut total_identifiers: usize = 0;

                            matches
                                .into_iter()
                                .map(|m| {
                                    let name =
                                        m.captures[0].node.utf8_text(&source.as_bytes()).unwrap();
                                    name.to_string()
                                })
                                .for_each(|name| {
                                    total_identifiers += 1;
                                    for keyword in keywords {
                                        if name.to_lowercase().contains(keyword) {
                                            *counts.get_mut(keyword).unwrap() += 1;
                                            found_identifiers.push(name.clone());
                                        }
                                    }
                                });
                            if found_identifiers.is_empty() {
                                None
                            } else {
                                Some((name.clone(), counts, found_identifiers, total_identifiers))
                            }
                        })
                        .collect();

                let call_counts: Vec<(String, HashMap<String, usize>, Vec<String>, usize)> =
                    contents
                        .iter()
                        .filter_map(|s| {
                            let language = self.language;
                            let mut parser = TSParser::new();
                            let query = Query::new(
                                language,
                                "(call_expression function: (identifier) @name ) @func",
                            )
                            .unwrap();
                            let mut querycursor = QueryCursor::new();
                            parser.set_language(language).unwrap();
                            let source = s.clone();
                            let tree = parser.parse(&s, None).unwrap();
                            let root = tree.root_node();
                            let matches =
                                querycursor.matches(&query, tree.root_node(), source.as_bytes());

                            let mut counts = HashMap::new();
                            // Add the keywords to counts with a count of 0
                            for keyword in keywords {
                                counts.insert(keyword.to_string(), 0);
                            }
                            let mut found_identifiers = Vec::new();
                            let mut total_identifiers: usize = 0;

                            matches
                                .into_iter()
                                .map(|m| {
                                    let name =
                                        m.captures[0].node.utf8_text(&source.as_bytes()).unwrap();
                                    name.to_string()
                                })
                                .for_each(|name| {
                                    total_identifiers += 1;
                                    for keyword in keywords {
                                        if name.to_lowercase().contains(keyword) {
                                            *counts.get_mut(keyword).unwrap() += 1;
                                            found_identifiers.push(name.clone());
                                        }
                                    }
                                });
                            if found_identifiers.is_empty() {
                                None
                            } else {
                                Some((name.clone(), counts, found_identifiers, total_identifiers))
                            }
                        })
                        .collect();
                self.cache.keywords.insert(name, (name_counts, call_counts));
            } else {
                eprintln!("{} is not a file", entry.path().display());
            }

            pb.inc(1);
        }

        self.cache.save().unwrap();

        pb.finish();

        Ok(())
    }

    async fn analyze(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Analyzing...");

        for keyword in &self.keywords {
            // Calculate how many of the packages contain at least one function with the keyword in the name
            let count = self
                .cache
                .keywords
                .iter()
                .map(|(_, (name_counts, call_counts))| {
                    (name_counts
                        .iter()
                        .map(|(_, counts, _, _)| counts.get(keyword).unwrap_or(&0))
                        .sum::<usize>()
                        > 0) as usize
                })
                .sum::<usize>();

            // Calculate how many average over all packages of how many functions have the keyword in the name divided by the total number of functions
            let avg_names = self
                .cache
                .keywords
                .iter()
                .map(|(_, (name_counts, _))| {
                    let has_kword = name_counts
                        .iter()
                        .map(|(_, counts, _, _)| *counts.get(keyword).unwrap_or(&0) as f64)
                        .sum::<f64>();
                    let total = name_counts
                        .iter()
                        .map(|(_, _, _, total)| *total as f64)
                        .sum::<f64>();
                    has_kword / total
                })
                .filter(|x| !x.is_nan())
                .sum::<f64>()
                / self.jobs as f64;

            let avg_calls = self
                .cache
                .keywords
                .iter()
                .map(|(_, (_, call_counts))| {
                    let has_kword = call_counts
                        .iter()
                        .map(|(_, counts, _, _)| *counts.get(keyword).unwrap_or(&0) as f64)
                        .sum::<f64>();
                    let total = call_counts
                        .iter()
                        .map(|(_, _, _, total)| *total as f64)
                        .sum::<f64>();
                    has_kword / total
                })
                .filter(|x| !x.is_nan())
                .sum::<f64>()
                / self.jobs as f64;

            let total_names = self
                .cache
                .keywords
                .iter()
                .map(|(_, (name_counts, _))| {
                    name_counts
                        .iter()
                        .map(|(_, _, _, total)| total)
                        .sum::<usize>()
                })
                .sum::<usize>();

            let total_calls = self
                .cache
                .keywords
                .iter()
                .map(|(_, (_, call_counts))| {
                    call_counts
                        .iter()
                        .map(|(_, _, _, total)| total)
                        .sum::<usize>()
                })
                .sum::<usize>();

            self.cache.results.insert(
                keyword.to_string(),
                (
                    count,
                    avg_names * 100.0,
                    avg_calls * 100.0,
                    total_names,
                    total_calls,
                ),
            );
        }

        self.cache.save().unwrap();

        Ok(())
    }

    async fn run(
        &mut self,
        download_dir: PathBuf,
        scan: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.downloadpkgs(download_dir).await?;
        if scan {
            if self.cache.keywords.is_empty() {
                self.scan().await?;
            }
            if self.cache.results.is_empty() {
                self.analyze().await?;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };

    let client = Client::new();

    if !args.output.exists() {
        create_dir_all(&args.output).unwrap();
    }

    let cache_loc = args.cache;

    let mut cache = if cache_loc.is_some() && cache_loc.as_ref().unwrap().exists() {
        Cache::from(cache_loc.clone().unwrap()).unwrap()
    } else {
        Cache::new()
    };

    if args.seed.is_some() {
        cache.seed = args.seed.unwrap() as isize;
    }

    cache.location = cache_loc;

    let mut keywords = HashSet::new();
    keywords.extend(args.keywords.iter().map(|s| s.to_lowercase()));

    let mut debbie = Debbie::new(
        args.count,
        client,
        rng,
        args.output.clone(),
        match args.language {
            Language::Ansic => "ansic".to_string(),
        },
        args.distro.unwrap_or("bullseye".to_string()),
        args.platform.unwrap_or("amd64".to_string()),
        cache,
        keywords,
    );

    debbie.run(args.output, args.scan).await?;

    Ok(())
}
