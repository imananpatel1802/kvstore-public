// server.rs
// TCP server that loads TreeMap from disk, persists after mutations.
use clap::{ArgAction, Parser};
use std::net::{TcpListener, TcpStream};
use std::io::{BufRead, BufReader, Write, Seek, SeekFrom};
use std::sync::{Arc, RwLock};
use kvstore::TreeMap;
use std::fs::*;


#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
   /// IP address and port to bind the server socket to
   #[arg(short, long, default_value = "127.0.0.1:4000")]
   addr: String,
 
   /// Do not persist the database contents to disk
   #[arg(short, long, default_value = "false")]
   memonly: bool,


   /// Run the server single-threaded
   #[arg(short, long, default_value_t = true, action = ArgAction::Set)]
   singlethread: bool,


   /// Location of the database file
   #[arg(short, long, default_value = "kvstore.db")]
   dbfile: String,


   /// Location of the database transaction log
   #[arg(short, long, default_value = "kvstore.log")]
   logfile: String,


   /// Pass this code to the server EXIT command to have it exit
   #[arg(short, long, default_value = "")]
   exit_code: String,  


   /// Number of modified batches before saving a snapshot and clearing the log
   #[arg(long, default_value = "1000", short = None)]
   snapshot_interval: u64,
}


fn handle_client(args: Arc<Args>, stream: TcpStream, map: Arc<RwLock<TreeMap<String, String>>>) {
   let mut writer = stream.try_clone().unwrap();
   let reader = BufReader::new(&stream);
   let mut lines = reader.lines();
   let mut response = String::new();
   let mut batch_modified = false; // Track if batch contains SET/REMOVE
   let mut batch_count = 0u64; // Track number of modified batches (Task 3, per fix)
   let mut log_entries = Vec::new(); // Buffer for batch-wise WAL
   let mut log_file = if !args.memonly {
       Some(OpenOptions::new()
           .append(true)
           .create(true)
           .open(&args.logfile)
           .unwrap())
   } else {
       None
   };


   while let Some(Ok(line)) = lines.next() {
      let parts: Vec<&str> = line.trim_end().splitn(3, ' ').collect();
      match parts[0] {
          "GET" if parts.len() == 2 => {
              let map = map.read().unwrap();
              response.push_str(&match map.get(&parts[1].to_string()) {
                  Some(v) => format!("OK {}\r\n", v),
                  None => "ERR NotFound\r\n".into(),
              });
          }
          "SET" if parts.len() == 3 => {
              let mut map = map.write().unwrap();
              map.insert(parts[1].to_string(), parts[2].to_string());
              batch_modified = true;
              log_entries.push(line); // Buffer without immediate clone + "\n" for efficiency
              response.push_str("OK\r\n");
          }
          "REMOVE" if parts.len() == 2 => {
              let mut map = map.write().unwrap();
              response.push_str(match map.remove(&parts[1].to_string()) {
                  Some(_) => {
                      batch_modified = true;
                      log_entries.push(line); // Buffer without immediate clone + "\n"
                      "OK\r\n"
                  }
                  None => "ERR NotFound\r\n",
              });
          }
          "SEEK" if parts.len() == 2 => {
              let map = map.read().unwrap();
              response.push_str(&match map.seek_ge(&parts[1].to_string()) {
                  Some((k, v)) => format!("OK {} {}\r\n", k, v),
                  None => "ERR NotFound\r\n".into(),
              });
          }
          "ENDBATCH" => {
              if !args.memonly && batch_modified {
                  // Write buffered log entries to disk ONCE per batch (Task 2.3 optimization)
                  if let Some(ref mut log_file) = log_file {
                      let log_data = log_entries.join("\n") + "\n"; // Concatenate once, add final \n
                      if let Err(e) = log_file.write_all(log_data.as_bytes()) {
                          eprintln!("Failed to write to log: {}", e);
                      }
                  }
                  // Increment batch count for modified batches (Task 3, per fix)
                  batch_count += 1;
                  // Snapshot logic (Task 3)
                  if batch_count >= args.snapshot_interval {
                      let map_guard = map.read().unwrap();
                      if let Err(e) = map_guard.save_to_file(&args.dbfile) {
                          eprintln!("Failed to save snapshot: {}", e);
                      }
                      // Drop read lock before log truncation (minor optimization)
                      drop(map_guard);
                      if let Some(ref mut log_file) = log_file {
                          if let Err(e) = log_file.seek(SeekFrom::Start(0)) {
                              eprintln!("Failed to rewind log: {}", e);
                          }
                          if let Err(e) = log_file.set_len(0) {
                              eprintln!("Failed to truncate log: {}", e);
                          }
                      }
                      batch_count = 0;
                  }
              }
              writer.write_all(response.as_bytes()).unwrap();
              response.clear();
              batch_modified = false;
              log_entries.clear();
          }
          "EXIT" if parts.len() == 2 && parts[1] == args.exit_code => {
              eprintln!("Received EXIT command with correct exit code. Exiting.");
              std::process::exit(0);
          }
          _ => {
              response.push_str("ERR UnknownCommand\r\n");
          }
      };
  }
}


fn recover_from_log(map: &mut TreeMap<String,String>, log: File) {
   let mut lines = BufReader::new(log).lines();
   println!("Recovering from log...");
   let mut count = 0;
   while let Some(Ok(line)) = lines.next() {
       count+=1;
       let parts: Vec<&str> = line.trim_end().splitn(3, ' ').collect();
       match parts[0] {
       "SET" if parts.len() == 3 => {
           map.insert(parts[1].to_string(), parts[2].to_string());
       },
       "REMOVE" if parts.len() == 2 => {
           map.remove(&parts[1].to_string());
       },
       _ => { panic!("Bad log entry."); }
       }
   }
   println!("Recovered {count} updates from log.\n");
}


fn main() -> std::io::Result<()> {
   let args = Arc::new(Args::parse());
 
   let mut map = match TreeMap::load_from_file(&args.dbfile) {
       Ok(m) => m,
       Err(_) => TreeMap::new(),
   };


   {   // Create or open pidfile and write PID to it
       let mut file = std::fs::File::create("server_pid.txt").unwrap();
       writeln!(file, "{}", std::process::id())?;
   }


   if let Ok(true) = std::fs::exists(args.logfile.as_str()) {
       recover_from_log(&mut map, File::open(args.logfile.as_str()).unwrap());
   }  


   let map = Arc::new(RwLock::new(map));


   let listener = TcpListener::bind(&args.addr)?;
   println!("Server listening on {}",args.addr);


   for stream in listener.incoming() {
       match stream {
           Ok(s) => {
               // Nagle's algorithm waits a little bit before acknowledging a received packet.
               // This is usually a good idea, but not if your program sends small packets and cares about low latency.
               s.set_nodelay(true)?;                
               let args = args.clone();
               let map = map.clone();


               // use the new --singlethread command line argument to set this
               if args.singlethread {
                   handle_client(args, s, map);
               }
               else {
                   std::thread::spawn(move || { handle_client(args, s, map); });
               }
           }
           Err(e) => eprintln!("Connection failed: {}", e),
       }
   }
   Ok(())
}


