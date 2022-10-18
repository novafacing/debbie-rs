use std::{collections::HashMap, fs::create_dir_all, path::PathBuf, thread::Thread};

use clap::{Parser, ValueEnum};
use futures::{stream::iter, StreamExt};
use indicatif::ProgressBar;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use reqwest::Client;
use serde::Deserialize;
use tl::{parse, Bytes, HTMLTag, Node, ParserOptions};
use tokio::{
    self,
    fs::File,
    io::{self, copy, AsyncWriteExt},
};
use tree_sitter::{Language as TSLanguage, Parser as TSParser};

const ALL_PKGS_URL: &str = "https://sources.debian.org/api/list";
const PKG_VERSIONS_URL: &str = "https://sources.debian.org/api/src/{packagename}/";
const PKG_INFO_URL: &str = "https://sources.debian.org/api/info/package/{packagename}/{version}/";
const STATUS_URL: &str = "https://sources.debian.org/api/ping/";
const PKG_SRC_URL: &str = "https://packages.debian.org/{distro}/{platform}/{packagename}";
const CONCURRENT_REQUESTS: usize = 32;

const TESTING_LIMIT: isize = -1;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, ValueEnum, Debug)]
enum Language {
    /// C
    Ansic,
}

#[derive(Parser, Debug)]
struct Args {
    /// Number of packages to search
    #[clap(short, long)]
    count: usize,
    /// Directory to output pcakges to
    #[clap(short, long)]
    output: PathBuf,
    /// An optional pre-existing download directory to use
    #[clap(short, long)]
    download_dir: Option<PathBuf>,
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

struct Debbie {
    jobs: usize,
    client: Client,
    rng: StdRng,
    outdir: PathBuf,
    lang: String,
    distro: String,
    platform: String,
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
    ) -> Self {
        Self {
            jobs,
            client,
            rng,
            outdir,
            lang,
            distro,
            platform,
        }
    }

    async fn getpkgs(&mut self) -> Result<Vec<String>, reqwest::Error> {
        let resp = self.client.get(ALL_PKGS_URL).send().await?;
        let resp: PackagesResponse = resp.json().await?;

        let pkgs = resp
            .packages
            .into_iter()
            .map(|pkg| pkg.name)
            // Do this later once we check if they are C
            // .choose_multiple(&mut self.rng, self.jobs);
            .collect::<Vec<_>>();

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

        Ok(pkgs_versions)
    }

    async fn getinfos(
        &mut self,
    ) -> Result<Vec<(String, String, HashMap<String, usize>)>, reqwest::Error> {
        let pkgs_versions = self.getversions().await?;
        println!("Getting infos for {} packages", pkgs_versions.len());
        let pb = ProgressBar::new(pkgs_versions.len() as u64);

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

        Ok(pkgs_infos)
    }

    async fn filterinfos(&mut self) -> Result<Vec<(String, String)>, reqwest::Error> {
        let pkgs_infos = self.getinfos().await.unwrap();
        println!("Filtering infos for {} packages", pkgs_infos.len());

        let lang = &self.lang;

        // Filter out non-primary lang packages
        let pkgs_infos = pkgs_infos
            .into_iter()
            .filter(|(_, _, sloc)| {
                sloc.iter().max_by(|a, b| a.1.cmp(&b.1)).map(|(k, _v)| k) == Some(lang)
            })
            .collect::<Vec<_>>();

        // Choose however many packages we are looking for
        Ok(pkgs_infos
            .into_iter()
            .map(|(name, version, _)| (name, version))
            .choose_multiple(&mut self.rng, self.jobs))
    }

    async fn getsrclinks(&mut self) -> Result<Vec<String>, reqwest::Error> {
        let pkgs_versions = self.filterinfos().await?;
        println!("Getting source links for {} packages", pkgs_versions.len());
        let pb = ProgressBar::new(pkgs_versions.len() as u64);

        let srclinks = iter(pkgs_versions)
            .map(|(name, _version)| {
                let client = &self.client;
                let distro = &self.distro;
                let platform = &self.platform;

                async move {
                    let url = PKG_SRC_URL
                        .replace("{distro}", distro)
                        .replace("{platform}", platform)
                        .replace("{packagename}", &name);
                    let resp = client.get(url).send().await?;
                    resp.text().await
                }
            })
            .buffer_unordered(CONCURRENT_REQUESTS)
            .filter_map(|pagetext| {
                pb.inc(1);
                async move {
                    match pagetext {
                        Ok(pagetext) => {
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
                                                    Some(alink)
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

        Ok(srclinks)
    }

    async fn write_file(&mut self, path: PathBuf, data: &[u8]) -> Result<(), tokio::io::Error> {
        let mut file = File::create(path).await?;
        file.write_all(data).await?;
        Ok(())
    }

    async fn download(&mut self) -> Result<(), reqwest::Error> {
        let srces = self.getsrclinks().await?;
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

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.download().await?)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let rng = match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };

    let client = Client::new();

    if !args.output.exists() {
        create_dir_all(&args.output).unwrap();
    }

    let mut debbie = Debbie::new(
        args.count,
        client,
        rng,
        args.output,
        match args.language {
            Language::Ansic => "ansic".to_string(),
        },
        args.distro.unwrap_or("bullseye".to_string()),
        args.platform.unwrap_or("amd64".to_string()),
    );

    println!("all: {:?}", debbie.run().await.unwrap());
}
