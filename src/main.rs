use std::{collections::HashMap, fs::create_dir_all, path::PathBuf, thread::Thread};

use clap::Parser;
use futures::{stream::iter, StreamExt};
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use reqwest::Client;
use serde::Deserialize;
use tokio;
use tree_sitter::{Language as TSLanguage, Parser as TSParser};

const ALL_PKGS_URL: &str = "https://sources.debian.org/api/list";
const PKG_VERSIONS_URL: &str = "https://sources.debian.org/api/src/{packagename}/";
const PKG_INFO_URL: &str = "https://sources.debian.org/api/info/package/{packagename}/{version}/";
const STATUS_URL: &str = "https://sources.debian.org/api/ping/";
const PKG_SRC_URL: &str = "https://packages.debian.org/{distro}/{platform}/{packagename}";
const CONCURRENT_REQUESTS: usize = 32;

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
    name: String,
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
enum SlocVecEntry {
    String(String),
    Number(usize),
}

#[derive(Deserialize)]
struct PackageInfoResponsePkgInfos {
    area: String,
    copyright: bool,
    ctags_count: usize,
    license: String,
    metric: PackageInfoResponsePkgInfosMetric,
    pts_link: String,
    sloc: Vec<Vec<SlocVecEntry>>,
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
}

impl Debbie {
    fn new(jobs: usize, client: Client, rng: StdRng, outdir: PathBuf) -> Self {
        Self {
            jobs,
            client,
            rng,
            outdir,
        }
    }

    async fn getpkgs(&self) -> Result<Vec<String>, reqwest::Error> {
        let resp = self.client.get(ALL_PKGS_URL).send().await?;
        let resp: PackagesResponse = resp.json().await?;

        let pkgs = resp
            .packages
            .into_iter()
            .map(|pkg| pkg.name)
            // Do this later once we check if they are C
            // .choose_multiple(&mut self.rng, self.jobs);
            .collect::<Vec<_>>();

        Ok(pkgs)
    }

    async fn getversions(&mut self) -> Result<Vec<(String, String)>, reqwest::Error> {
        let pkgs = self.getpkgs().await?;

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
            .filter_map(|versions| async move {
                match versions {
                    Ok(versions) => {
                        if let Some(version) = versions
                            .versions
                            .iter()
                            .filter(|v| v.suites.contains(&"bullseye".to_string()))
                            .next()
                        {
                            Some((versions.package.clone(), version.version.clone()))
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            })
            .collect::<Vec<_>>()
            .await;

        Ok(pkgs_versions)
    }

    async fn getinfos(
        &mut self,
    ) -> Result<Vec<(String, String, HashMap<String, usize>)>, reqwest::Error> {
        let pkgs_versions = self.getversions().await?;

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
            .filter_map(|info| async move {
                match info {
                    Ok(info) => {
                        let mut sloc = HashMap::new();
                        for entry in info.pkg_infos.sloc {
                            if let [SlocVecEntry::String(lang), SlocVecEntry::Number(count)] =
                                entry.as_slice()
                            {
                                sloc.insert(lang.clone(), *count);
                            }
                        }
                        Some((info.package, info.version, sloc))
                    }
                    Err(_) => None,
                }
            })
            .collect::<Vec<_>>()
            .await;

        Ok(pkgs_infos)
    }

    async fn run(&self, urls: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
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

    let mut debbie = Debbie::new(args.count, client, rng, args.output);

    let pkgs = debbie.getpkgs().await.unwrap();

    println!("{:?}", pkgs);

    debbie.run(pkgs).await.unwrap();
}
