use std::{error::Error, fs};

use std::sync::mpsc;
use std::sync::mpsc::sync_channel;
use std::thread;

use oxrdf::*;
use oxrdfio::{RdfFormat, RdfSerializer};
use regex::Regex;
use spareval::QueryEvaluator;
use spareval::QueryResults;
use spargebra::SparqlParser;

use csv::ReaderBuilder;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write, stdin, stdout};
use std::process::exit;

use clap::{Arg, ArgAction, ArgMatches, command, value_parser};

#[allow(dead_code)]
#[derive(Default)]
pub struct OxiGen {
    pub delimiter: String,
    pub tab: bool,
    pub test: u32,
    pub headers: bool,
    pub escape_char: String,
    pub quote_char: String,
    pub normalize: bool,
    pub gzip: bool,
    pub ntriples: bool,
    pub dedup: u32,
    pub bind_empty_strings: bool,
    pub input: String,
    pub output: String,
    pub query: String,
    pub split: Vec<(String, String, String)>,

    // NEW: optional named graph
    pub graph: Option<String>,
}

impl OxiGen {
    pub fn transform(&mut self) -> Result<(), Box<dyn Error>> {
        let num_workers: usize = num_cpus::get();

        let mut csv_senders = vec![];
        let mut csv_receivers = vec![];
        for (sender, receiver) in (0..num_workers).map(|_| sync_channel(100)) {
            csv_senders.push(sender);
            csv_receivers.push(receiver);
        }
        let (triple_tx, triple_rx) = mpsc::channel();

        let query_str = fs::read_to_string(&self.query).unwrap();
        let query = match SparqlParser::new()
            .with_prefix("tarql", "https://semanticarts.com/tarql/")?
            .parse_query(&query_str)
        {
            Ok(qr) => qr,
            Err(e) => {
                eprintln!("SPARQL Syntax Error in query: {:?}", e);
                exit(-1);
            }
        };

        let prefixes = extract_prefixes(&query_str).to_owned();
        let query_vars = extract_variables(&query_str);
        let bind_empty = self.bind_empty_strings;

        let mut transformers = Vec::with_capacity(num_workers);
        for _tid in 0..num_workers {
            let triple_tx = triple_tx.clone();
            let receiver = csv_receivers.pop().unwrap();
            let p1 = prefixes.clone();
            let p2 = prefixes.clone();
            let evaluator = QueryEvaluator::new()
                .with_custom_function(
                    NamedNode::new("https://semanticarts.com/tarql/expandPrefix")?,
                    move |args| args.first().map(|p| expand_prefix(&p1, p).unwrap()),
                )
                .with_custom_function(
                    NamedNode::new("https://semanticarts.com/tarql/expandPrefixedName")?,
                    move |args| args.first().and_then(|p| expand_prefixed_name(&p2, p)),
                );
            let query = query.clone();
            let query_vars = query_vars.clone();
            transformers.push(thread::spawn(move || {
                let empty_store = Dataset::new();
                while let Ok((row, unwrapped)) = receiver.recv() {
                    let mut row_triples: Vec<Triple> = vec![];
                    for unwrapped_row in unwrapped {
                        let mut prepared = evaluator.prepare(&query);
                        for (varname, value) in unwrapped_row {
                            if query_vars.contains(&varname) {
                                let value_str: String = value;
                                if bind_empty || !value_str.trim().is_empty() {
                                    prepared = prepared.substitute_variable(
                                        Variable::new(varname).unwrap(),
                                        Literal::from(value_str),
                                    );
                                }
                            }
                        }

                        if query_vars.contains("ROWNUM") {
                            prepared = prepared.substitute_variable(
                                Variable::new("ROWNUM").unwrap(),
                                Literal::from(row),
                            );
                        }

                        let results = prepared.execute(&empty_store);
                        if let QueryResults::Graph(triples) = results.unwrap() {
                            row_triples.extend(triples.into_iter().map(|t| t.unwrap()));
                        }
                    }
                    triple_tx.send(row_triples).unwrap();
                }
                drop(triple_tx);
            }));
        }

        let output_path = self.output.clone();
        let compress = self.gzip;

        // NEW: conditional N-Quads
        let output_format = if self.graph.is_some() {
            RdfFormat::NQuads
        } else if self.ntriples {
            RdfFormat::NTriples
        } else {
            RdfFormat::Turtle
        };

        let graph_name = self.graph.clone();
        let dedup = self.dedup;
        let test_rows = self.test;

        let writer_task = thread::spawn(move || {
            let mut out_writer: BufWriter<Box<dyn Write>> =
                BufWriter::new(match output_path.as_ref() {
                    "STDOUT" => Box::new(stdout()) as Box<dyn Write>,
                    _ => {
                        if compress {
                            let out_fh = File::create(&output_path).unwrap();
                            let out_gz = GzEncoder::new(out_fh, Compression::default());
                            Box::new(BufWriter::new(out_gz))
                        } else {
                            let out_fh = File::create(&output_path).unwrap();
                            Box::new(BufWriter::new(out_fh))
                        }
                    }
                });

            let mut first_time = true;

            // NEW: always store quads
            let mut store = HashSet::<Quad>::new();

            while let Ok(row_triples) = triple_rx.recv() {
                for t in row_triples {
                    let g = if let Some(ref iri) = graph_name {
                        NamedNode::new(iri.clone()).unwrap().into()
                    } else {
                        GraphName::DefaultGraph
                    };

                    let q = Quad::new(t.subject, t.predicate, t.object, g);
                    store.insert(q);
                }

                if !store.is_empty() && (dedup == 0 || store.len() >= dedup as usize) {
                    flush_store(&mut store, &mut out_writer, output_format, &prefixes, first_time)
                        .unwrap();
                    first_time = false;
                }
            }

            if dedup > 0 && !store.is_empty() {
                flush_store(&mut store, &mut out_writer, output_format, &prefixes, first_time)
                    .unwrap();
            }

            out_writer.flush().expect("Error flushing output");
        });
        // Create CSV reader based on command line options
        let input_reader: Box<dyn Read + Send> = if self.input == "STDIN" {
            Box::new(BufReader::with_capacity(100000, stdin()))
        } else {
            Box::new(BufReader::with_capacity(
                100000,
                File::open(&self.input).unwrap(),
            ))
        };
        let mut rdr = ReaderBuilder::new()
            .has_headers(self.headers)
            .delimiter(match self.tab {
                true => b'\t',
                _ => self.delimiter.chars().next().unwrap() as u8,
            })
            .quote(self.quote_char.chars().next().unwrap() as u8)
            .escape(Some(self.escape_char.chars().next().unwrap() as u8))
            .from_reader(input_reader);
        let normalize = self.normalize;
        let has_headers = self.headers;
        let split = self.split.clone();

        let reader_task = thread::spawn(move || {
            let mut headers = Vec::new();
            if has_headers {
                let header = rdr.headers().unwrap().clone();
                for field in &header {
                    headers.push(clean_column(field, &normalize).to_string());
                }
            } else {
                let alphabet_column_names: Vec<String> = ('a'..='z')
                    .chain('A'..='Z')
                    .map(|c| c.to_string())
                    .collect();
                headers = alphabet_column_names.clone();
            }

            let mut row = 0;
            let mut transformer = 0;
            for result in rdr.records() {
                if test_rows != 0 && row >= test_rows {
                    break;
                }

                let record: Vec<String> = match result {
                    Ok(r) => r.iter().map(|s| s.to_string()).collect(),
                    Err(e) => {
                        eprintln!("Error reading row {}: {:?}", row, e);
                        exit(-1);
                    }
                };

                let unwrapped = apply_split(&split, &record, &headers);
                csv_senders[transformer].send((row, unwrapped)).unwrap();
                transformer = (transformer + 1) % num_workers;
                row += 1;
            }
            for channel in csv_senders {
                drop(channel);
            }
        });

        let reader_result = reader_task.join();
        let transformer_results: Vec<_> = transformers.into_iter().map(|t| t.join()).collect();
        drop(triple_tx);
        let writer_result = writer_task.join();

        if let Err(e) = writer_result {
            eprintln!("Writer thread panicked: {:?}", e);
            return Err("Writer thread panicked".into());
        }
        for (i, result) in transformer_results.into_iter().enumerate() {
            if let Err(e) = result {
                eprintln!("Transformer thread {} panicked: {:?}", i, e);
                return Err("Transformer thread panicked".into());
            }
        }
        if let Err(e) = reader_result {
            eprintln!("Reader thread panicked: {:?}", e);
            return Err("Reader thread panicked".into());
        }

        Ok(())
    }
}

fn apply_split<'a>(
    split: &[(String, String, String)],
    record: &'a [String],
    headers: &'a [String],
) -> Vec<Vec<(String, String)>> {
    let mut bindings: Vec<Vec<(String, String)>> = vec![
        headers
            .iter()
            .cloned()
            .zip(record.iter().cloned())
            .collect(),
    ];
    for (original, split, delimiter) in split.iter() {
        let original_idx = match headers.iter().position(|h| h == original) {
            None => continue,
            Some(idx) => idx,
        };
        let mut next_vals: Vec<Vec<(String, String)>> = vec![];
        for val_set in bindings {
            let original_val = &val_set[original_idx].1;
            for split_val in original_val.split(delimiter) {
                let mut modified_row = val_set.clone();
                modified_row.push((split.clone(), split_val.to_string()));
                next_vals.push(modified_row);
            }
        }
        bindings = next_vals;
    }
    bindings
}


pub fn flush_store<W: Write>(
    store: &mut HashSet<Quad>,
    out_writer: &mut BufWriter<W>,
    format: RdfFormat,
    prefixes: &HashMap<String, String>,
    first_time: bool,
) -> Result<(), Box<dyn Error + 'static>> {
    let mut config = RdfSerializer::from_format(format);

    // Prefixes only apply to Turtle
    if format == RdfFormat::Turtle {
        for (prefix, iri) in prefixes {
            config = config.with_prefix(prefix, iri).expect("Invalid prefix IRI");
        }
    }

    let mut serializer = config.for_writer(Vec::new());

    // Sort only for Turtle
    if format == RdfFormat::Turtle {
        let mut sorted: Vec<_> = store.iter().collect();
        sorted.sort_by_key(|q| {
            (
                q.subject.to_string(),
                q.predicate.to_string(),
                q.object.to_string(),
                q.graph_name.to_string(),
            )
        });

        for quad in sorted.iter() {
            serializer.serialize_quad(*quad)?; // deref &&Quad → &Quad
        }
    } else {
        // N-Triples or N-Quads
        for quad in store.iter() {
            serializer.serialize_quad(quad)?; // deref &Quad → QuadRef
        }
    }

    let mut rdf_str = serializer.finish().unwrap();

    // Remove prefix lines after first Turtle block
    if !first_time && format == RdfFormat::Turtle {
        while rdf_str.starts_with(b"@prefix") {
            if let Some(pos) = rdf_str.iter().position(|c| *c == b'\n') {
                rdf_str = rdf_str.split_off(pos + 1);
            } else {
                rdf_str.clear();
                break;
            }
        }
    }

    out_writer.write_all(&rdf_str)?;
    store.clear();
    Ok(())
}


fn expand_prefix(prefixes: &HashMap<String, String>, prefix: &Term) -> Option<Term> {
    let prefix_name = match prefix {
        Term::Literal(l) => l.value().to_string(),
        _ => {
            eprintln!("Invalid argument passed to expandPrefix: {:?}", prefix);
            exit(-1);
        }
    };
    prefixes
        .get(&prefix_name)
        .map(|iri| Term::Literal(Literal::from(iri.to_string())))
}

fn expand_prefixed_name(prefixes: &HashMap<String, String>, qname: &Term) -> Option<Term> {
    let qname_str = match qname {
        Term::Literal(l) => l.value().to_string(),
        _ => {
            eprintln!("Invalid argument passed to expandPrefixedName: {:?}", qname);
            exit(-1);
        }
    };
    if qname_str.is_empty() {
        return None;
    }
    let (prefix_name, rest) = qname_str.split_at(match qname_str.find(':') {
        Some(offset) => offset,
        _ => {
            eprintln!("Malformed QName in expandPrefixedName: {:?}", &qname_str);
            return None;
        }
    });
    prefixes.get(prefix_name).map(|pref_iri| {
        Term::NamedNode(NamedNode::new(pref_iri.to_string() + rest.get(1..).unwrap()).unwrap())
    })
}

fn extract_prefixes(query_text: &str) -> HashMap<String, String> {
    let mut prefix_map = HashMap::new();

    let re = Regex::new(r"\b[pP][rR][eE][fF][iI][xX]\s+(\S*?):\s+<(.+?)>").unwrap();
    for (_, [prefix, iri]) in re.captures_iter(query_text).map(|c| c.extract()) {
        prefix_map.insert(String::from(prefix), String::from(iri));
    }
    prefix_map
}

fn extract_variables(query_text: &str) -> HashSet<String> {
    let re = Regex::new(r"\?([A-Za-z_][A-Za-z_0-9]*?)[^A-Za-z_0-9]").unwrap();
    let without_comments: String = query_text
        .lines()
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<&str>>()
        .join("\n");
    re.captures_iter(&without_comments)
        .map(|c| c.extract())
        .map(|(_, [varname])| varname.to_string())
        .collect()
}

fn clean_column(field: &str, normalize: &bool) -> String {
    if *normalize {
        field.trim().to_uppercase().replace('\"', "")
    } else {
        field.trim().replace('\"', "")
    }
}

pub fn parse_args<I>(args: I) -> ArgMatches
where
    I: IntoIterator<Item = String>,
{
    command!()
        .about("oxi-gen: Convert CSV file to RDF using SPARQL")
        .arg(
            Arg::new("delimiter")
                .short('d')
                .long("delimiter")
                .default_value(",")
                .conflicts_with("tab")
                .help("Delimiting character of the input file"),
        )
        .arg(
            Arg::new("tab")
                .short('t')
                .long("tab")
                .action(ArgAction::SetTrue)
                .conflicts_with("delimiter")
                .help("Is the Input tab-separated (TSV)?"),
        )
        .arg(
            Arg::new("escape_char")
                .short('p')
                .long("escape_char")
                .default_value("\\")
                .help("Escape character used in the input file"),
        )
        .arg(
            Arg::new("quote_char")
                .long("quote_char")
                .default_value("\"")
                .help("Quote character used in the input file"),
        )
        .arg(
            Arg::new("normalize")
                .short('n')
                .long("normalize")
                .action(ArgAction::SetTrue)
                .help("Normalize column names to UPPERCASE"),
        )
        .arg(
            Arg::new("headers")
                .short('H')
                .long("no-header-row")
                .action(ArgAction::SetFalse)
                .help("File has headers in the first row [default: True]"),
        )
        .arg(
            Arg::new("gzip")
                .short('g')
                .long("gzip")
                .action(ArgAction::SetTrue)
                .requires("output")
                .help("gzip file output"),
        )
        .arg(
            Arg::new("ntriples")
                .long("ntriples")
                .action(ArgAction::SetTrue)
                .help("Emit N-Triples [default: Turtle]"),
        )
        .arg(
            Arg::new("test")
                .long("test")
                .value_parser(value_parser!(u32).range(1..50))
                .action(ArgAction::Set)
                .num_args(0..=1)
                .require_equals(true)
                .default_missing_value("5")
                .help("Show output for first TEST records"),
        )
        .arg(
            Arg::new("split")
                .long("split")
                .action(ArgAction::Append)
                .num_args(3)
                .value_names(["ORIGINAL", "SPLIT", "DELIMITER"])
                .help("Split column ORIGINAL into multiple values"),
        )
        .arg(
            Arg::new("dedup")
                .long("dedup")
                .value_parser(value_parser!(u32).range(1000..=5000000))
                .default_missing_value("1000")
                .num_args(0..=1)
                .require_equals(true)
                .action(ArgAction::Set)
                .help("Window size for duplicate removal"),
        )
        .arg(
            Arg::new("bind_empty_strings")
                .long("bind-empty-strings")
                .action(ArgAction::SetTrue)
                .help("Bind empty CSV values as empty string literals"),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .action(ArgAction::Set)
                .default_value("STDOUT")
                .help("Output file (default: STDOUT)"),
        )
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .action(ArgAction::Set)
                .default_value("STDIN")
                .help("Input CSV file (default: STDIN)"),
        )
        .arg(
            Arg::new("query")
                .short('q')
                .long("query")
                .action(ArgAction::Set)
                .required(true)
                .help("SPARQL query file"),
        )
        // NEW: optional named graph
        .arg(
            Arg::new("graph")
                .long("graph")
                .action(ArgAction::Set)
                .help("Named graph IRI (enables N-Quads output)"),
        )
        .get_matches_from(args)
}

pub fn configure_transform<I>(args: I) -> OxiGen
where
    I: IntoIterator<Item = String>,
{
    let matches = parse_args(args);

    let split_def = match matches.get_many::<String>("split") {
        None => vec![],
        Some(splits) => {
            let mut sval_it = splits.cloned();
            let mut split_defs = Vec::<(String, String, String)>::new();
            while let Some(orig) = sval_it.next() {
                split_defs.push((orig, sval_it.next().unwrap(), sval_it.next().unwrap()));
            }
            split_defs
        }
    };

    OxiGen {
        delimiter: matches.get_one::<String>("delimiter").unwrap().to_string(),
        tab: matches.get_flag("tab"),
        test: matches.get_one::<u32>("test").copied().unwrap_or(0),
        headers: matches.get_flag("headers"),
        escape_char: matches.get_one::<String>("escape_char").unwrap().to_string(),
        quote_char: matches.get_one::<String>("quote_char").unwrap().to_string(),
        normalize: matches.get_flag("normalize"),
        gzip: matches.get_flag("gzip"),
        ntriples: matches.get_flag("ntriples"),
        dedup: matches.get_one::<u32>("dedup").copied().unwrap_or(0),
        bind_empty_strings: matches.get_flag("bind_empty_strings"),
        input: matches.get_one::<String>("input").unwrap().to_string(),
        output: matches.get_one::<String>("output").unwrap().to_string(),
        query: matches.get_one::<String>("query").unwrap().to_string(),
        split: split_def,

        // NEW
        graph: matches.get_one::<String>("graph").cloned(),
    }
}
