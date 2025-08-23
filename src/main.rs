use base64::Engine;
use base64::engine::general_purpose;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::str::FromStr;

/// CLI arguments
struct Args {
    csv_file: String,
    index_name: String,
    host: String,
    batch_size: usize,
    user: Option<String>,
    password: Option<String>,
}

fn parse_args() -> Args {
    let mut csv_file = String::new();
    let mut index_name = String::new();
    let mut host = String::from("http://localhost:9200");
    let mut batch_size = 1000;
    let mut user: Option<String> = None;
    let mut password: Option<String> = None;

    let mut it = env::args().skip(1).peekable();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--host" => {
                if let Some(v) = it.next() {
                    host = v;
                }
            }
            "--batch-size" => {
                if let Some(v) = it.next() {
                    batch_size = v.parse().unwrap_or(1000);
                }
            }
            "--user" => {
                if let Some(v) = it.next() {
                    user = Some(v);
                }
            }
            "--pass" => {
                if let Some(v) = it.next() {
                    password = Some(v);
                }
            }
            _ if csv_file.is_empty() => csv_file = arg,
            _ if index_name.is_empty() => index_name = arg,
            _ => {}
        }
    }

    if csv_file.is_empty() || index_name.is_empty() {
        eprintln!(
            "Usage: elastic_importer <csv_file> <index_name> [--host http://localhost:9200] [--batch-size 1000] [--user USER --pass PASS]"
        );
        std::process::exit(1);
    }

    Args {
        csv_file,
        index_name,
        host,
        batch_size,
        user,
        password,
    }
}

/// HTTP target struct
struct HttpTarget {
    host: String,
    port: u16,
    base_path: String,
}

fn parse_http_target(url: &str) -> Result<HttpTarget, String> {
    let prefix = "http://";
    if !url.starts_with(prefix) {
        return Err("Only http:// supported".into());
    }
    let rest = &url[prefix.len()..];
    let parts: Vec<&str> = rest.splitn(2, '/').collect();
    let host_port = parts[0];
    let base_path = if parts.len() == 2 {
        format!("/{}", parts[1])
    } else {
        String::new()
    };

    let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
        let port = p.parse::<u16>().map_err(|_| "Invalid port")?;
        (h.to_string(), port)
    } else {
        (host_port.to_string(), 9200)
    };

    Ok(HttpTarget {
        host,
        port,
        base_path,
    })
}

/// Detect JSON type: number, bool, string
fn infer_type(s: &str) -> String {
    if s.is_empty() {
        return "null".into();
    }
    if let Ok(i) = i64::from_str(s) {
        return i.to_string();
    }
    if let Ok(f) = f64::from_str(s) {
        return f.to_string();
    }
    match s.to_lowercase().as_str() {
        "true" => "true".into(),
        "false" => "false".into(),
        _ => format!("\"{}\"", json_escape(s)),
    }
}

/// Escape string for JSON
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

/// CSV reader
struct CsvReader {
    buf: String,
    idx: usize,
}

impl CsvReader {
    fn new(mut reader: impl BufRead) -> io::Result<Self> {
        let mut buf = String::new();
        reader.read_to_string(&mut buf)?;
        Ok(Self { buf, idx: 0 })
    }

    fn next_record(&mut self) -> Option<Vec<String>> {
        if self.idx >= self.buf.len() {
            return None;
        }
        let bytes = self.buf.as_bytes();
        let mut fields = Vec::new();
        let mut field = String::new();
        let mut in_quotes = false;
        let mut i = self.idx;

        while i < bytes.len() {
            let c = bytes[i] as char;
            if in_quotes {
                match c {
                    '"' => {
                        if i + 1 < bytes.len() && bytes[i + 1] as char == '"' {
                            field.push('"');
                            i += 2;
                        } else {
                            in_quotes = false;
                            i += 1;
                        }
                    }
                    _ => {
                        field.push(c);
                        i += 1;
                    }
                }
            } else {
                match c {
                    '"' => {
                        in_quotes = true;
                        i += 1;
                    }
                    ',' => {
                        fields.push(field.clone());
                        field.clear();
                        i += 1;
                    }
                    '\n' => {
                        fields.push(field.clone());
                        field.clear();
                        i += 1;
                        break;
                    }
                    '\r' => {
                        if i + 1 < bytes.len() && bytes[i + 1] as char == '\n' {
                            fields.push(field.clone());
                            field.clear();
                            i += 2;
                            break;
                        } else {
                            fields.push(field.clone());
                            field.clear();
                            i += 1;
                            break;
                        }
                    }
                    _ => {
                        field.push(c);
                        i += 1;
                    }
                }
            }
        }

        if i >= bytes.len() && (!field.is_empty() || !fields.is_empty()) {
            fields.push(field);
        }

        self.idx = i;
        if fields.is_empty() && self.idx >= bytes.len() {
            None
        } else {
            Some(fields)
        }
    }
}

/// CSV iterator with headers
struct CsvIter {
    rdr: CsvReader,
    headers: Vec<String>,
}

impl CsvIter {
    fn from_reader(r: impl BufRead) -> io::Result<Self> {
        let mut rdr = CsvReader::new(r)?;
        let headers = rdr.next_record().unwrap_or_default();
        Ok(Self { rdr, headers })
    }
}

impl Iterator for CsvIter {
    type Item = Vec<(String, String)>;
    fn next(&mut self) -> Option<Self::Item> {
        let rec = self.rdr.next_record()?;
        if rec.is_empty() {
            return None;
        }
        let mut row = Vec::with_capacity(self.headers.len());
        for (i, name) in self.headers.iter().enumerate() {
            let val = rec.get(i).map(|s| s.trim()).unwrap_or("");
            row.push((name.clone(), val.to_string()));
        }
        Some(row)
    }
}

/// Convert dict to JSON with type inference
fn dict_to_json(row: &[(String, String)]) -> String {
    let mut out = String::from("{");
    let mut first = true;
    for (k, v) in row {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        out.push_str(&json_escape(k));
        out.push_str("\":");
        out.push_str(&infer_type(v));
    }
    out.push('}');
    out
}

/// Send bulk request to ES
fn http_post_bulk(
    target: &HttpTarget,
    bulk_path: &str,
    body: &str,
    auth: Option<(String, String)>,
) -> Result<String, String> {
    let addr = format!("{}:{}", target.host, target.port);
    let mut stream = TcpStream::connect(&addr).map_err(|e| format!("connect error: {}", e))?;
    let mut request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\nContent-Length: {}\r\n",
        bulk_path,
        target.host,
        body.as_bytes().len()
    );

    if let Some((user, pass)) = auth {
        let token = general_purpose::STANDARD.encode(format!("{}:{}", user, pass));
        request.push_str(&format!("Authorization: Basic {}\r\n", token));
    }

    request.push_str("\r\n");
    request.push_str(body);

    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("write error: {}", e))?;
    stream.flush().map_err(|e| format!("flush error: {}", e))?;
    let mut resp = String::new();
    stream
        .read_to_string(&mut resp)
        .map_err(|e| format!("read error: {}", e))?;
    Ok(resp)
}

/// Ping ES
fn es_ping(target: &HttpTarget, auth: Option<(String, String)>) -> bool {
    let addr = format!("{}:{}", target.host, target.port);
    if let Ok(mut stream) = TcpStream::connect(&addr) {
        let path = if target.base_path.is_empty() {
            "/"
        } else {
            &target.base_path
        };
        let mut req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            path, target.host
        );
        if let Some((u, p)) = auth {
            let token = general_purpose::STANDARD.encode(format!("{}:{}", u, p));
            req.push_str(&format!("Authorization: Basic {}\r\n", token));
        }
        req.push_str("\r\n");
        if stream.write_all(req.as_bytes()).is_ok() {
            let mut resp = String::new();
            if stream.read_to_string(&mut resp).is_ok() {
                return resp.starts_with("HTTP/1.1 200");
            }
        }
    }
    false
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let target =
        parse_http_target(&args.host).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    if !Path::new(&args.csv_file).exists() {
        return Err(format!("CSV file not found: {}", args.csv_file).into());
    }

    let auth = match (&args.user, &args.password) {
        (Some(u), Some(p)) => Some((u.clone(), p.clone())),
        _ => None,
    };

    if !es_ping(&target, auth.clone()) {
        return Err(format!("Cannot connect to ES at {}", args.host).into());
    }

    let file = File::open(&args.csv_file)?;
    let reader = BufReader::new(file);
    let mut csv = CsvIter::from_reader(reader)?;

    let bulk_path = format!("{}/_bulk", target.base_path);
    let mut batch: Vec<String> = Vec::with_capacity(args.batch_size * 2);
    let mut total_docs = 0;

    while let Some(row) = csv.next() {
        batch.push(format!(
            "{{\"index\":{{\"_index\":\"{}\"}}}}",
            args.index_name
        ));
        batch.push(dict_to_json(&row));

        if batch.len() / 2 >= args.batch_size {
            let mut body = batch.join("\n");
            body.push('\n');
            let resp = http_post_bulk(&target, &bulk_path, &body, auth.clone())?;
            if resp.contains("\"errors\":true") {
                eprintln!("Bulk errors detected");
            }
            total_docs += batch.len() / 2;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let mut body = batch.join("\n");
        body.push('\n');
        let resp = http_post_bulk(&target, &bulk_path, &body, auth.clone())?;
        if resp.contains("\"errors\":true") {
            eprintln!("Bulk errors detected");
        }
        total_docs += batch.len() / 2;
    }

    println!(
        "Successfully uploaded {} documents to index: {}",
        total_docs, args.index_name
    );
    Ok(())
}
