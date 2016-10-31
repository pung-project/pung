extern crate pung;
extern crate getopts;
extern crate timely;

// standard libraries


use getopts::Options;

use pung::db;
use pung::server::send_dataflow;
use std::cell::RefCell;
use std::rc::Rc;
use std::str::FromStr;

macro_rules! timely_opt {
    ($matches:ident, $list:ident, $opt:expr) => {{
        if let Some(x) = $matches.opt_str($opt) {
            $list.push(format!("-{}", $opt));
            $list.push(x);
        }
    }};
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {

    let args: Vec<String> = std::env::args().collect();
    let program = args[0].clone();
    let mut timely_args: Vec<String> = Vec::new();


    let mut opts = Options::new();
    opts.optflag("", "help", "print this help menu");

    // timely parameters
    opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
    opts.optopt("p", "process", "identity of this process", "IDX");
    opts.optopt("n", "processes", "number of processes", "NUM");
    opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
    opts.optflag("r", "report", "reports connection progress");

    // pung parameters
    opts.optopt("i", "ip", "address of pung RPC", "IP");
    opts.optopt("s", "port", "initial port of pung RPC", "PORT");
    opts.optopt("k", "buckets", "number of buckets", "BUCKETS");
    //    opts.optopt("a", "alpha", "PIR aggregation", "ALPHA");
    opts.optopt("d", "depth", "PIR depth", "DEPTH");
    opts.optopt("b", "extra", "extra tuples added", "EXTRA");
    opts.optopt("m", "messages", "min messages", "MESSAGES");
    opts.optopt("o", "opt", "power (p) or hybrid (h)", "p / h");
    opts.optopt("t", "type", "retrieval type", "e / b / t");

    // Parse parameters
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => {
            print_usage(&program, opts);
            panic!(e.to_string());
        }
    };

    if matches.opt_present("help") {
        print_usage(&program, opts);
        return;
    }

    // process timely parameters
    timely_opt!(matches, timely_args, "w");
    timely_opt!(matches, timely_args, "p");
    timely_opt!(matches, timely_args, "n");
    timely_opt!(matches, timely_args, "h");

    if matches.opt_present("r") {
        timely_args.push("-r".to_string());
    }

    // process pung parameters
    let rpc_addr: String = match matches.opt_str("i") {
        Some(v) => v,
        None => "127.0.0.1".to_string(),
    };

    let port: usize = match matches.opt_str("s") {
        Some(v) => usize::from_str_radix(&v, 10).unwrap(),
        None => 12345,
    };

    let buckets: usize = match matches.opt_str("k") {
        Some(v) => usize::from_str_radix(&v, 10).unwrap(),
        None => 1,
    };

    //    let alpha: u64 = match matches.opt_str("a") {
    // Some(v) => u64::from_str_radix(&v, 10).unwrap(),
    // None => 1,
    // };
    //

    let depth: u64 = match matches.opt_str("d") {
        Some(v) => u64::from_str_radix(&v, 10).unwrap(),
        None => 1,
    };

    let extra_tuples: usize = match matches.opt_str("b") {
        Some(v) => usize::from_str_radix(&v, 10).unwrap(),
        None => 0,
    };

    let min_messages: u32 = match matches.opt_str("m") {
        Some(v) => u32::from_str_radix(&v, 10).unwrap(),
        None => 1,
    };

    let ret_scheme: db::RetScheme = match matches.opt_str("t") {
        Some(v) => {
            match v.as_ref() {
                "e" => db::RetScheme::Explicit,
                "b" => db::RetScheme::Bloom,
                "t" => db::RetScheme::Tree,
                _ => panic!("Invalid retrieval parameters {}. Choose either e, b, or t.", v),
            }
        }

        None => db::RetScheme::Explicit,
    };

    let opt_scheme: db::OptScheme = match matches.opt_str("o") {
        Some(v) => {
            match v.as_ref() {
                "p" => db::OptScheme::Aliasing,
                "h2" => db::OptScheme::Hybrid2,
                "h4" => db::OptScheme::Hybrid4,
                _ => panic!("Invalid optimization parameters {}. Choose either p or h.", v),
            }
        }

        None => db::OptScheme::Normal,
    };

    // For each worker thred
    timely::execute_from_args(timely_args.into_iter(), move |mut worker| {

            let index = worker.index();
            let dbase = Rc::new(RefCell::new(db::Database::new(ret_scheme, opt_scheme, buckets, depth)));

            let send_handle = send_dataflow::graph(&mut worker, dbase.clone(), buckets);

            let worker_port = port + index; // port of this worker
            let addr = FromStr::from_str(&format!("{}:{}", &rpc_addr, worker_port)).unwrap();

            // Run RPC server on this worker.
            pung::server::run_rpc(addr,
                                  worker.clone(),
                                  send_handle,
                                  dbase,
                                  extra_tuples,
                                  min_messages,
                                  opt_scheme);

        })
        .expect("Timely dataflow error");
}
