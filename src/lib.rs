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
        // oxigraph does not allow for specifying variable substitution unless
        // the variable is referenced in the query. Extract anything that looks like
        // a variable identifier, and then filter out columns that are not used
        let query_vars = extract_variables(&query_str);
        let bind_empty = self.bind_empty_strings;

        let mut transformers = Vec::with_capacity(num_workers);
        for _tid in 0..num_workers {
            let triple_tx = triple_tx.clone();
            let receiver = csv_receivers.pop().unwrap();
            // Each captured context gets its own copy of the prefixes
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
                let mut processed = 0;
                // eprintln!("Transformer {} started", tid);                
                let empty_store = Dataset::new();
                while let Ok((row, unwrapped)) = receiver.recv() {
                    // eprintln!("Received {}: {:?}", row, &unwrapped);
                    let mut row_triples: Vec<Triple> = vec![];
                    for unwrapped_row in unwrapped {
                        let mut prepared = evaluator.prepare(&query);
                        for (varname, value) in unwrapped_row {
                            if query_vars.contains(&varname) {
                                // Skip empty values unless bind_empty is true
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
                            // store.extend(triples.into_iter().map(|t| t.unwrap()));
                        }
                    }
                    // eprintln!("Sending {}: {:?}", row, &row_triples);
                    triple_tx.send(row_triples).unwrap();
                    processed += 1;
                    if processed % 50000 == 0 {
                        // eprintln!("Transformer {tid} processed {processed} rows");
                    }                    
                }
                drop(triple_tx);
                // eprintln!("Transformer {tid} finished {processed} rows");
            }));
            // eprintln!("Transformer {tid} spawned");
        }

        let output_path = self.output.clone();
        let compress = self.gzip;

        // Named graph forces N-Quads output; single graph shared by every triple
        let output_format = if self.graph.is_some() {
            RdfFormat::NQuads
        } else if self.ntriples {
            RdfFormat::NTriples
        } else {
            RdfFormat::Turtle
        };

        let graph_name = self
            .graph
            .as_ref()
            .map(|iri| NamedNode::new(iri.clone()).unwrap());
        let dedup = self.dedup;
        let test_rows = self.test;

        let writer_task = thread::spawn(move || {
            // eprintln!("Writer started");
            // Open the output file. Will use the filename if given or STDOUT if not

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
            // Track first output, to only emit prefixes once to Turtle
            let mut first_time = true;

            let mut store = HashSet::<Triple>::new();

            while let Ok(row_triples) = triple_rx.recv() {
                // eprintln!("Received {}: {:?}", &row_triples);
                store.extend(row_triples);

                if !store.is_empty() && (dedup == 0 || store.len() >= dedup as usize) {
                    flush_store(
                        &mut store,
                        &mut out_writer,
                        output_format,
                        &prefixes,
                        first_time,
                        graph_name.as_ref(),
                    )
                    .unwrap();
                    first_time = false;
                }
            }
            // If deduplicating, flush remaining store to output
            if dedup > 0 && !store.is_empty() {
                flush_store(
                    &mut store,
                    &mut out_writer,
                    output_format,
                    &prefixes,
                    first_time,
                    graph_name.as_ref(),
                )
                .unwrap();
            }

            out_writer.flush().expect("Error flushing to output file");
        });
        // eprintln!("Writer spawned");

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
           // eprintln!("Reader started");
            // Extract headers from the CSV, unless --no-header-row is used, in
            // which case columns are aliased to 'a'..'z', 'A'..'Z' (max 52 columns)
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
            // let should_pass: Vec<bool> = headers.iter().map(|h| query_vars.contains(h)).collect();
            let mut row = 0;
            let mut transformer = 0;
            for result in rdr.records() {
                // Check for test rows in the reader, and stop sending
                if test_rows != 0 && row >= test_rows {
                    break;
                }

                // The iterator yields Result<StringRecord, Error>, so we check the
                // error here.

                let record: Vec<String> = match result {
                    Ok(r) => r.iter().map(|s| s.to_string()).collect(),
                    Err(e) => {
                        eprintln!("Error reading row {}: {:?}", row, e);
                        exit(-1);
                    }
                };

                let unwrapped = apply_split(&split, &record, &headers);
                // eprintln!("Sending {:?}", &unwrapped);
                csv_senders[transformer].send((row, unwrapped)).unwrap();
                transformer = (transformer + 1) % num_workers;
                row += 1;
            }
                if row % 50000 == 0 {
                    // eprintln!("Sent {row} rows");
                }

            for channel in csv_senders {
                drop(channel);
            }
        });
        // eprintln!("Reader spawned");

        let reader_result = reader_task.join();
        let transformer_results: Vec<_> = transformers.into_iter().map(|t| t.join()).collect();
        drop(triple_tx);
        let writer_result = writer_task.join();

        // Check for thread panics and report them clearly
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
    store: &mut HashSet<Triple>,
    out_writer: &mut BufWriter<W>,
    format: RdfFormat,
    prefixes: &HashMap<String, String>,
    first_time: bool,
    graph_name: Option<&NamedNode>,
) -> Result<(), Box<dyn Error + 'static>> {
    let mut config = RdfSerializer::from_format(format);

    // Prefixes only apply to Turtle
    if format == RdfFormat::Turtle {
        for (prefix, iri) in prefixes {
            config = config.with_prefix(prefix, iri).expect("Invalid prefix IRI");
        }
    }

    let mut serializer = config.for_writer(Vec::new());

    let graph_ref: GraphNameRef = match graph_name {
        Some(node) => node.as_ref().into(),
        None => GraphNameRef::DefaultGraph,
    };

    // Sort only for Turtle
    if format == RdfFormat::Turtle {
        let mut sorted: Vec<_> = store.iter().collect();
        sorted.sort_by_key(|t| {
            (
                t.subject.to_string(),
                t.predicate.to_string(),
                t.object.to_string(),
            )
        });

        for triple in sorted.iter() {
            serializer.serialize_triple(*triple)?;
        }
    } else if format == RdfFormat::NQuads {
        for triple in store.iter() {
            serializer.serialize_quad(triple.as_ref().in_graph(graph_ref))?;
        }
    } else {
        // N-Triples
        for triple in store.iter() {
            serializer.serialize_triple(triple)?;
        }
    }

    let mut rdf_str = serializer.finish().unwrap();

    // Remove prefix lines after first Turtle block
    if !first_time && format == RdfFormat::Turtle {
        // Remove all leading prefix lines
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
                .help(
                    "Normalize column names - convert all to UPPERCASE [default: no normalization]",
                ),
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
                .help("gzip file output. Output file name must be provided"),
        )
        .arg(
            Arg::new("ntriples")
                .long("ntriples")
                .action(ArgAction::SetTrue)
                .help("Emit N-Triples [default: turtle]"),
        )
        .arg(
            Arg::new("test")
                .long("test")
                .value_parser(value_parser!(u32).range(1..50))
                .action(ArgAction::Set)
                .num_args(0..=1)
                .require_equals(true)
                .default_missing_value("5")
                .help("Show output for first TEST records (default=5)"),
        )
        .arg(
            Arg::new("split")
                .long("split")
                .action(ArgAction::Append)
                .num_args(3)
                .value_names(["ORIGINAL", "SPLIT", "DELIMITER"])
                .help("Split column ORIGINAL into multiple values in SPLIT on DELIMITER"),
        )
        .arg(
            Arg::new("dedup")
                .long("dedup")
                .value_parser(value_parser!(u32).range(1000..=5000000))
                .default_missing_value("1000")
                .num_args(0..=1)
                .require_equals(true)
                .action(ArgAction::Set)
                .help("Window size in which to remove duplicate triples (default=1000)"),
        )
        .arg(
            Arg::new("bind_empty_strings")
                .long("bind-empty-strings")
                .action(ArgAction::SetTrue)
                .help(
                    "Bind empty CSV values as empty string literals (default: skip empty values)",
                ),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .action(ArgAction::Set)
                .default_value("STDOUT")
                .help("File to write to, omit to use STDOUT"),
        )
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .action(ArgAction::Set)
                .default_value("STDIN")
                .help("CSV to be processed, omit to use STDIN"),
        )
        .arg(
            Arg::new("query")
                .short('q')
                .long("query")
                .action(ArgAction::Set)
                .required(true)
                .help("File containing a SPARQL query to be applied to an input file (required)"),
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
        test: match matches.get_one::<u32>("test") {
            None => 0,
            Some(t) => *t,
        },
        headers: matches.get_flag("headers"),
        escape_char: matches
            .get_one::<String>("escape_char")
            .unwrap()
            .to_string(),
        quote_char: matches.get_one::<String>("quote_char").unwrap().to_string(),
        normalize: matches.get_flag("normalize"),
        gzip: matches.get_flag("gzip"),
        ntriples: matches.get_flag("ntriples"),
        dedup: match matches.get_one::<u32>("dedup") {
            None => 0,
            Some(t) => *t,
        },
        bind_empty_strings: matches.get_flag("bind_empty_strings"),
        input: matches.get_one::<String>("input").unwrap().to_string(),
        output: matches.get_one::<String>("output").unwrap().to_string(),
        query: matches.get_one::<String>("query").unwrap().to_string(),
        split: split_def,

        // NEW
        graph: matches.get_one::<String>("graph").cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_column_no_normalize() {
        assert_eq!(clean_column("  column_name  ", &false), "column_name");
        assert_eq!(clean_column("\"quoted\"", &false), "quoted");
        assert_eq!(clean_column("  \"spaces\"  ", &false), "spaces");
        assert_eq!(clean_column("MixedCase", &false), "MixedCase");
    }

    #[test]
    fn test_clean_column_normalize() {
        assert_eq!(clean_column("  column_name  ", &true), "COLUMN_NAME");
        assert_eq!(clean_column("\"quoted\"", &true), "QUOTED");
        assert_eq!(clean_column("MixedCase", &true), "MIXEDCASE");
        assert_eq!(clean_column("lower", &true), "LOWER");
    }

    #[test]
    fn test_extract_prefixes_basic() {
        let query = r#"
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT * WHERE { ?s ?p ?o }
        "#;
        let prefixes = extract_prefixes(query);
        assert_eq!(prefixes.len(), 2);
        assert_eq!(
            prefixes.get("rdf"),
            Some(&"http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string())
        );
        assert_eq!(
            prefixes.get("rdfs"),
            Some(&"http://www.w3.org/2000/01/rdf-schema#".to_string())
        );
    }

    #[test]
    fn test_extract_prefixes_case_insensitive() {
        let query = r#"
            prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PrEfIx dc: <http://purl.org/dc/elements/1.1/>
        "#;
        let prefixes = extract_prefixes(query);
        assert_eq!(prefixes.len(), 3);
        assert!(prefixes.contains_key("rdf"));
        assert!(prefixes.contains_key("foaf"));
        assert!(prefixes.contains_key("dc"));
    }

    #[test]
    fn test_extract_prefixes_empty() {
        let query = "SELECT * WHERE { ?s ?p ?o }";
        let prefixes = extract_prefixes(query);
        assert_eq!(prefixes.len(), 0);
    }

    #[test]
    fn test_extract_variables_basic() {
        let query = "SELECT ?name ?age WHERE { ?person foaf:name ?name . ?person foaf:age ?age }";
        let vars = extract_variables(query);
        assert!(vars.contains("name"));
        assert!(vars.contains("age"));
        assert!(vars.contains("person"));
    }

    #[test]
    fn test_extract_variables_underscores() {
        let query = "SELECT ?first_name ?last_name WHERE { ?s ?p ?o }";
        let vars = extract_variables(query);
        assert!(vars.contains("first_name"));
        assert!(vars.contains("last_name"));
        assert!(vars.contains("s"));
    }

    #[test]
    fn test_extract_variables_with_numbers() {
        let query = "SELECT ?var1 ?var2 ?var123 WHERE { ?s ?p ?o }";
        let vars = extract_variables(query);
        assert!(vars.contains("var1"));
        assert!(vars.contains("var2"));
        assert!(vars.contains("var123"));
    }

    #[test]
    fn test_extract_variables_empty() {
        let query = "SELECT * WHERE { <http://example.org/s> <http://example.org/p> <http://example.org/o> }";
        let vars = extract_variables(query);
        // Should be empty or nearly empty since no variables are used
        assert!(!vars.contains("SELECT"));
        assert!(!vars.contains("WHERE"));
    }

    #[test]
    fn test_extract_variables_with_comments() {
        let query = r#"
            SELECT ?name ?age WHERE {
                # This is a comment with ?commented_var
                ?person foaf:name ?name .
                  # Another comment with ?another_commented_var
                ?person foaf:age ?age
            }
        "#;
        let vars = extract_variables(query);
        assert!(vars.contains("name"));
        assert!(vars.contains("age"));
        assert!(vars.contains("person"));
        // Variables in comments should not be extracted
        assert!(!vars.contains("commented_var"));
        assert!(!vars.contains("another_commented_var"));
    }

    #[test]
    fn test_expand_prefix_valid() {
        let mut prefixes = HashMap::new();
        prefixes.insert(
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        );
        prefixes.insert("foaf".to_string(), "http://xmlns.com/foaf/0.1/".to_string());

        let prefix_term = Term::Literal(Literal::from("rdf"));
        let result = expand_prefix(&prefixes, &prefix_term);
        assert!(result.is_some());
        if let Some(Term::Literal(lit)) = result {
            assert_eq!(lit.value(), "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        } else {
            panic!("Expected literal term");
        }
    }

    #[test]
    fn test_expand_prefix_unknown_prefix() {
        let prefixes = HashMap::new();
        let prefix_term = Term::Literal(Literal::from("unknown"));
        let result = expand_prefix(&prefixes, &prefix_term);
        assert!(result.is_none());
    }

    #[test]
    fn test_expand_prefixed_name_valid() {
        let mut prefixes = HashMap::new();
        prefixes.insert(
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        );
        prefixes.insert("foaf".to_string(), "http://xmlns.com/foaf/0.1/".to_string());

        let qname = Term::Literal(Literal::from("foaf:name"));
        let result = expand_prefixed_name(&prefixes, &qname);
        assert!(result.is_some());
        if let Some(Term::NamedNode(node)) = result {
            assert_eq!(node.as_str(), "http://xmlns.com/foaf/0.1/name");
        } else {
            panic!("Expected named node term");
        }
    }

    #[test]
    fn test_expand_prefixed_name_rdf_type() {
        let mut prefixes = HashMap::new();
        prefixes.insert(
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        );

        let qname = Term::Literal(Literal::from("rdf:type"));
        let result = expand_prefixed_name(&prefixes, &qname);
        assert!(result.is_some());
        if let Some(Term::NamedNode(node)) = result {
            assert_eq!(
                node.as_str(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            );
        } else {
            panic!("Expected named node term");
        }
    }

    #[test]
    fn test_expand_prefixed_name_no_prefix() {
        let prefixes = HashMap::new();
        let qname = Term::Literal(Literal::from("foaf:name"));
        let result = expand_prefixed_name(&prefixes, &qname);
        assert!(result.is_none());
    }

    #[test]
    fn test_expand_prefixed_name_no_colon() {
        let mut prefixes = HashMap::new();
        prefixes.insert(
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        );

        let qname = Term::Literal(Literal::from("nocolon"));
        let result = expand_prefixed_name(&prefixes, &qname);
        assert!(result.is_none());
    }

    #[test]
    fn test_expand_prefixed_name_empty() {
        let mut prefixes = HashMap::new();
        prefixes.insert(
            "rdf".to_string(),
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
        );
        let qname = Term::Literal(Literal::from(""));
        let result = expand_prefixed_name(&prefixes, &qname);
        assert!(
            result.is_none(),
            "expandPrefixedName should return None for empty parameter"
        );
    }

    #[test]
    fn test_apply_split_no_split() {
        let split: Vec<(String, String, String)> = vec![];
        let headers = vec!["col1".to_string(), "col2".to_string()];
        let record = vec!["value1".to_string(), "value2".to_string()];

        let result = apply_split(&split, &record, &headers);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
        assert_eq!(result[0][0], ("col1".to_string(), "value1".to_string()));
        assert_eq!(result[0][1], ("col2".to_string(), "value2".to_string()));
    }

    #[test]
    fn test_apply_split_single_split() {
        let split = vec![("tags".to_string(), "tag".to_string(), ",".to_string())];
        let headers = vec!["name".to_string(), "tags".to_string()];
        let record = vec!["Alice".to_string(), "rust,python,go".to_string()];

        let result = apply_split(&split, &record, &headers);
        assert_eq!(result.len(), 3); // 3 tags split

        // Check first row
        assert_eq!(result[0][0], ("name".to_string(), "Alice".to_string()));
        assert_eq!(
            result[0][1],
            ("tags".to_string(), "rust,python,go".to_string())
        );
        assert_eq!(result[0][2], ("tag".to_string(), "rust".to_string()));

        // Check second row
        assert_eq!(result[1][0], ("name".to_string(), "Alice".to_string()));
        assert_eq!(result[1][2], ("tag".to_string(), "python".to_string()));

        // Check third row
        assert_eq!(result[2][2], ("tag".to_string(), "go".to_string()));
    }

    #[test]
    fn test_apply_split_multiple_splits() {
        let split = vec![
            ("colors".to_string(), "color".to_string(), ",".to_string()),
            ("sizes".to_string(), "size".to_string(), ";".to_string()),
        ];
        let headers = vec![
            "name".to_string(),
            "colors".to_string(),
            "sizes".to_string(),
        ];
        let record = vec![
            "Product".to_string(),
            "red,blue".to_string(),
            "S;M".to_string(),
        ];

        let result = apply_split(&split, &record, &headers);
        // 2 colors × 2 sizes = 4 combinations
        assert_eq!(result.len(), 4);

        // Verify all have the color and size fields
        for row in result.iter() {
            assert!(row.iter().any(|(k, _)| k == "color"));
            assert!(row.iter().any(|(k, _)| k == "size"));
        }
    }

    #[test]
    fn test_apply_split_nonexistent_column() {
        let split = vec![(
            "nonexistent".to_string(),
            "split_val".to_string(),
            ",".to_string(),
        )];
        let headers = vec!["col1".to_string(), "col2".to_string()];
        let record = vec!["value1".to_string(), "value2".to_string()];

        let result = apply_split(&split, &record, &headers);
        // Should return original row since column doesn't exist
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 2);
    }

    #[test]
    fn test_flush_store_ntriples() {
        use std::io::Cursor;

        let mut store = HashSet::new();
        let triple1 = Triple::new(
            NamedNode::new("http://example.org/s1").unwrap(),
            NamedNode::new("http://example.org/p").unwrap(),
            NamedNode::new("http://example.org/o1").unwrap(),
        );
        let triple2 = Triple::new(
            NamedNode::new("http://example.org/s2").unwrap(),
            NamedNode::new("http://example.org/p").unwrap(),
            NamedNode::new("http://example.org/o2").unwrap(),
        );
        store.insert(triple1);
        store.insert(triple2);

        let buffer = Vec::new();
        let cursor = Cursor::new(buffer);
        let mut writer = BufWriter::new(Box::new(cursor) as Box<dyn Write>);

        let result = flush_store(
            &mut store,
            &mut writer,
            RdfFormat::NTriples,
            &HashMap::new(),
            true,
            None,
        );
        assert!(result.is_ok());
        assert_eq!(store.len(), 0); // Store should be cleared
    }

    #[test]
    fn test_integration_nquads_named_graph() {
        use oxrdfio::RdfParser;
        use std::path::PathBuf;

        let temp_file = std::env::temp_dir().join("oxi_gen_test_nquads.nq");
        let _ = std::fs::remove_file(&temp_file);

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let input_path = manifest_dir.join("tests/fixtures/optional_field.csv");
        let query_path = manifest_dir.join("tests/fixtures/optional_field.rq");

        let graph_iri = "http://example.org/graph";

        let args = vec![
            "oxi_gen".to_string(),
            "--input".to_string(),
            input_path.to_str().unwrap().to_string(),
            "--query".to_string(),
            query_path.to_str().unwrap().to_string(),
            "--output".to_string(),
            temp_file.to_str().unwrap().to_string(),
            "--graph".to_string(),
            graph_iri.to_string(),
        ];

        let mut transform = configure_transform(args);
        let result = transform.transform();
        assert!(
            result.is_ok(),
            "Transform should succeed: {:?}",
            result.err()
        );

        let file = File::open(&temp_file).expect("Should open output file");
        let parser = RdfParser::from_format(RdfFormat::NQuads).for_reader(file);

        let mut quad_count = 0;
        for q in parser {
            let quad = q.expect("Failed to parse N-Quads output");
            assert_eq!(
                quad.graph_name.to_string(),
                format!("<{}>", graph_iri),
                "Every quad should be placed in the named graph"
            );
            quad_count += 1;
        }

        let _ = std::fs::remove_file(&temp_file);

        // Same fixture as optional_field.rq/csv: row 1 (non-empty alt_label) -> 3 triples,
        // row 2 (empty alt_label, skipped by default) -> 2 triples = 5 total
        assert_eq!(quad_count, 5, "Expected 5 quads, got {}", quad_count);
    }
}