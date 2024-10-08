use bincode::{Decode, Encode};
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Encode, Decode)]
struct Column {
    name: String,
    data: Vec<i32>,
}

#[derive(Serialize, Deserialize, Clone, Encode, Decode)]
struct Table {
    name: String,
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Clone, Encode, Decode)]
struct Database {
    name: String,
    tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize, Clone, Encode, Decode)]
struct ServerState {
    current_db: Option<Database>,
    results: HashMap<String, Vec<i32>>,
    float_results: HashMap<String, Vec<f64>>,
}

fn main() -> io::Result<()> {
    println!("Current working directory: {:?}", std::env::current_dir()?);

    let state = Arc::new(Mutex::new(load_database().unwrap_or_else(|_| {
        ServerState {
            current_db: None,
            results: HashMap::new(),
            float_results: HashMap::new(),
        }
    })));

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        println!("Ctrl-C received, shutting down...");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let listener = TcpListener::bind("127.0.0.1:8080")?;
    listener.set_nonblocking(true)?;

    println!("Server listening on 127.0.0.1:8080");

    while running.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                println!("New client connected");
                let state_clone = Arc::clone(&state);
                let running_clone = Arc::clone(&running);
                std::thread::spawn(move || {
                    handle_client(stream, state_clone, running_clone);
                });
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => eprintln!("Error accepting connection: {}", e),
        }

        // Check if we should shut down
        if !running.load(Ordering::SeqCst) {
            break;
        }
    }

    println!("Server shutting down");
    save_database(&state.lock().unwrap())?;

    // Ensure all client threads have a chance to finish
    std::thread::sleep(Duration::from_secs(1));

    Ok(())
}

fn send_response(stream: &mut TcpStream, response: &str) -> io::Result<()> {
    let response_len = response.len() as u32;
    stream.write_all(&response_len.to_be_bytes())?;
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn handle_client(mut stream: TcpStream, state: Arc<Mutex<ServerState>>, running: Arc<AtomicBool>) {
    stream
        .set_nonblocking(false)
        .expect("Failed to set blocking");
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut buffer = String::new();

    while running.load(Ordering::SeqCst) {
        buffer.clear();
        match reader.read_line(&mut buffer) {
            Ok(0) => {
                println!("Client disconnected");
                break;
            }
            Ok(_) => {
                println!("Received command: {}", buffer.trim());
                let response = {
                    let mut state_guard = state.lock().unwrap();
                    process_command(buffer.trim(), &mut state_guard, &running, &mut stream)
                };

                println!("Processed command, sending response");
                if let Err(e) = send_response(&mut stream, &response) {
                    eprintln!("Failed to send response: {}", e);
                    break;
                }
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                continue;
            }
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }

        if !running.load(Ordering::SeqCst) {
            println!("Server is shutting down, closing client connection.");
            break;
        }
    }
    println!("Client handler exiting");
}

fn process_command(
    command: &str,
    state: &mut ServerState,
    running: &Arc<AtomicBool>,
    stream: &mut TcpStream,
) -> String {
    println!("DEBUG: Processing command: {}", command);
    let parts: Vec<&str> = command.split('=').collect();

    if parts.len() == 2 {
        let destination = parts[0].trim();
        let operation = parts[1].trim();

        if operation.starts_with("select(") {
            return handle_select(destination, &operation[7..operation.len() - 1], state);
        } else if operation.starts_with("fetch(") {
            return handle_fetch(destination, &operation[6..operation.len() - 1], state);
        } else if operation.starts_with("avg(") {
            return handle_avg(destination, &operation[4..operation.len() - 1], state);
        }
    }

    let parts: Vec<&str> = command.split('(').collect();

    match parts[0].trim() {
        "create" => handle_create(parts[1].trim_end_matches(')'), state),
        "load" => {
            println!("DEBUG: Detected load command");
            handle_load(parts[1].trim_end_matches(')'), state, stream)
        }
        "print" => handle_print(parts[1].trim_end_matches(')'), state),
        "show_tables" => show_tables(state),
        "display_table" | "display" => {
            if parts.len() > 1 {
                let table_name = parts[1]
                    .trim_end_matches(')')
                    .trim_matches('\'')
                    .trim_matches('"');
                display_table(table_name, state)
            } else {
                "-- Error: Invalid display_table command\n".to_string()
            }
        }
        "shutdown" => handle_shutdown(state, running),
        "debug" => debug_database(state),
        _ => format!("-- Unknown command: {}\n", command),
    }
}

fn debug_database(state: &ServerState) -> String {
    if let Some(db) = &state.current_db {
        let mut output = format!("Current database: {}\n", db.name);
        output.push_str(&format!("Number of tables: {}\n", db.tables.len()));
        output.push_str("Tables:\n");
        for (table_name, table) in &db.tables {
            output.push_str(&format!(
                "  - {} (Columns: {})\n",
                table_name,
                table.columns.len()
            ));
            for column in &table.columns {
                output.push_str(&format!(
                    "    * {} (Rows: {})\n",
                    column.name,
                    column.data.len()
                ));
            }
        }
        output
    } else {
        "No active database\n".to_string()
    }
}

fn handle_create(args: &str, state: &mut ServerState) -> String {
    let parts: Vec<&str> = args.split(',').collect();
    match parts[0] {
        "db" => create_db(parts[1].trim_matches('"'), state),
        "tbl" => {
            let table_name = parts[1].trim_matches('"');
            let db_name = parts[2];
            let col_count: usize = parts[3].parse().unwrap();
            create_table(table_name, db_name, col_count, state)
        }
        "col" => {
            let col_name = parts[1].trim_matches('"');
            let table_ref = parts[2];
            create_column(col_name, table_ref, state)
        }
        _ => "-- Invalid create command\n".to_string(),
    }
}

fn create_db(name: &str, state: &mut ServerState) -> String {
    state.current_db = Some(Database {
        name: name.to_string(),
        tables: HashMap::new(),
    });
    "-- Database created\n".to_string()
}

fn create_table(name: &str, db_name: &str, col_count: usize, state: &mut ServerState) -> String {
    if let Some(db) = &mut state.current_db {
        if db.name != db_name {
            return "-- Error: Database mismatch\n".to_string();
        }
        let table = Table {
            name: name.to_string(),
            columns: Vec::with_capacity(col_count),
        };
        db.tables.insert(name.to_string(), table);
        "-- Table created\n".to_string()
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn create_column(col_name: &str, table_ref: &str, state: &mut ServerState) -> String {
    if let Some(db) = &mut state.current_db {
        let parts: Vec<&str> = table_ref.split('.').collect();
        if parts.len() != 2 || parts[0] != db.name {
            return "-- Error: Invalid table reference\n".to_string();
        }
        if let Some(table) = db.tables.get_mut(parts[1]) {
            let column = Column {
                name: col_name.to_string(),
                data: Vec::new(),
            };
            table.columns.push(column);
            "-- Column created\n".to_string()
        } else {
            "-- Error: Table not found\n".to_string()
        }
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn handle_load(args: &str, state: &mut ServerState, stream: &mut TcpStream) -> String {
    println!("Entering handle_load with args: {}", args);

    let file_size: usize = match args.trim().parse() {
        Ok(size) => {
            println!("Parsed file size: {} bytes", size);
            size
        }
        Err(e) => {
            println!("Error parsing file size: {}", e);
            return format!("-- Error: Invalid file size: {}\n", e);
        }
    };

    // Send acknowledgment to client
    if let Err(e) = stream.write_all(b"ACK") {
        println!("Error sending acknowledgment: {}", e);
        return format!("-- Error sending acknowledgment: {}\n", e);
    }
    if let Err(e) = stream.flush() {
        println!("Error flushing stream after acknowledgment: {}", e);
        return format!("-- Error flushing stream: {}\n", e);
    }

    println!("Attempting to read {} bytes from stream", file_size);
    let mut file_contents = vec![0u8; file_size];

    match stream.read_exact(&mut file_contents) {
        Ok(_) => {
            println!("Successfully read {} bytes", file_size);
            println!(
                "First 100 bytes of file contents: {:?}",
                &file_contents[..100.min(file_contents.len())]
            );
            println!("Calling load_data");
            load_data(&file_contents, state)
        }
        Err(e) => {
            println!("Error reading file contents: {}", e);
            format!("-- Error reading file contents: {}\n", e)
        }
    }
}

fn load_data(file_contents: &[u8], state: &mut ServerState) -> String {
    println!("Entering load_data");

    if let Some(db) = &mut state.current_db {
        println!("Current database: {}", db.name);

        let reader = BufReader::new(file_contents);
        let mut lines = reader.lines();

        // Read header
        match lines.next() {
            Some(Ok(header)) => {
                println!("Header: {}", header);
                let column_names: Vec<&str> = header.split(',').collect();
                println!("Column names: {:?}", column_names);
            }
            Some(Err(e)) => {
                println!("Error reading header: {}", e);
                return format!("-- Error reading header: {}\n", e);
            }
            None => {
                println!("Error: Empty file");
                return "-- Error: Empty file\n".to_string();
            }
        }

        // Assume we're loading into the last created table
        if let Some(table) = db.tables.values_mut().last() {
            println!("Loading into table: {}", table.name);
            let mut row_count = 0;

            for (i, line) in lines.enumerate() {
                match line {
                    Ok(line) => {
                        let values: Vec<i32> = line
                            .split(',')
                            .map(|s| {
                                s.parse().unwrap_or_else(|e| {
                                    println!("Error parsing value in row {}: {}", i + 2, e);
                                    0 // or some default value
                                })
                            })
                            .collect();

                        if i < 5 {
                            // Print first 5 rows for debugging
                            println!("Row {}: {:?}", i + 2, values);
                        }

                        for (j, value) in values.iter().enumerate() {
                            if j < table.columns.len() {
                                table.columns[j].data.push(*value);
                            }
                        }
                        row_count += 1;
                    }
                    Err(e) => {
                        println!("Error reading line {}: {}", i + 2, e);
                    }
                }
            }

            println!("Loaded {} rows into table '{}'", row_count, table.name);
            format!(
                "-- Data loaded into table '{}'. {} rows inserted.\n",
                table.name, row_count
            )
        } else {
            println!("Error: No tables in the current database");
            "-- Error: No tables in the current database\n".to_string()
        }
    } else {
        println!("Error: No active database");
        "-- Error: No active database\n".to_string()
    }
}

fn handle_print(args: &str, state: &ServerState) -> String {
    let result_name = args.trim();

    if let Some(result) = state.results.get(result_name) {
        // Find the maximum number of digits
        let max_digits = result
            .iter()
            .map(|&value| value.to_string().len())
            .max()
            .unwrap_or(0);

        let output: String = result
            .iter()
            .map(|&value| format!("{:>width$}\n", value, width = max_digits))
            .collect();

        // Add an extra newline at the end
        format!("{}\n", output)
    } else if let Some(float_result) = state.float_results.get(result_name) {
        println!("DEBUG: Found float result");

        // Handle float results
        float_result
            .iter()
            .map(|&value| format!("{:.2}\n", value))
            .collect()
    } else {
        format!("-- Error: Result '{}' not found", result_name)
    }
}

fn handle_select(destination: &str, args: &str, state: &mut ServerState) -> String {
    let parts: Vec<&str> = args.split(',').collect();
    if parts.len() != 3 {
        return "-- Error: Invalid select command\n".to_string();
    }

    let col_name = parts[0].trim();
    let low = if parts[1].trim() == "null" {
        None
    } else {
        parts[1].trim().parse().ok()
    };
    let high = if parts[2].trim() == "null" {
        None
    } else {
        parts[2].trim().parse().ok()
    };

    select(destination, col_name, low, high, state)
}

fn select(
    destination: &str,
    col_name: &str,
    low: Option<i32>,
    high: Option<i32>,
    state: &mut ServerState,
) -> String {
    if let Some(db) = &mut state.current_db {
        let parts: Vec<&str> = col_name.split('.').collect();
        if parts.len() != 3 || parts[0] != db.name {
            return "-- Error: Invalid column reference\n".to_string();
        }
        if let Some(table) = db.tables.get(parts[1]) {
            if let Some(column) = table.columns.iter().find(|c| c.name == parts[2]) {
                let result: Vec<i32> = column
                    .data
                    .iter()
                    .enumerate()
                    .filter(|&(_, &value)| {
                        (low.is_none() || value >= low.unwrap())
                            && (high.is_none() || value < high.unwrap())
                    })
                    .map(|(index, _)| index as i32)
                    .collect();

                state
                    .results
                    .insert(destination.to_string(), result.clone());

                format!("-- Selected {} rows\n", result.len())
            } else {
                "-- Error: Column not found\n".to_string()
            }
        } else {
            "-- Error: Table not found\n".to_string()
        }
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn handle_fetch(destination: &str, args: &str, state: &mut ServerState) -> String {
    let parts: Vec<&str> = args.split(',').collect();
    if parts.len() != 2 {
        return "-- Error: Invalid fetch command\n".to_string();
    }

    let col_name = parts[0].trim();
    let pos_name = parts[1].trim();

    fetch(destination, col_name, pos_name, state)
}

fn fetch(destination: &str, col_name: &str, pos_name: &str, state: &mut ServerState) -> String {
    if let Some(db) = &state.current_db {
        let parts: Vec<&str> = col_name.split('.').collect();
        if parts.len() != 3 || parts[0] != db.name {
            return "-- Error: Invalid column reference\n".to_string();
        }
        if let Some(table) = db.tables.get(parts[1]) {
            if let Some(column) = table.columns.iter().find(|c| c.name == parts[2]) {
                if let Some(positions) = state.results.get(pos_name) {
                    let result: Vec<i32> = positions
                        .iter()
                        .filter_map(|&pos| {
                            let usize_pos = pos as usize;
                            column.data.get(usize_pos).copied()
                        })
                        .collect();
                    let result_len = result.len();
                    state.results.insert(destination.to_string(), result);
                    format!("-- Fetched {} values\n", result_len)
                } else {
                    "-- Error: Position result not found\n".to_string()
                }
            } else {
                "-- Error: Column not found\n".to_string()
            }
        } else {
            "-- Error: Table not found\n".to_string()
        }
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn handle_shutdown(state: &mut ServerState, running: &Arc<AtomicBool>) -> String {
    running.store(false, Ordering::SeqCst);
    save_database(state).unwrap_or_else(|e| eprintln!("Error saving database: {}", e));
    "-- Shutting down the server".to_string()
}

fn handle_avg(destination: &str, args: &str, state: &mut ServerState) -> String {
    let source = args.trim();
    println!("DEBUG: Calculating average for '{}'", source);
    if let Some(result) = state.results.get(source) {
        let sum: i64 = result.iter().map(|&x| x as i64).sum();
        let count = result.len();
        if count > 0 {
            let average = (sum as f64) / (count as f64);
            let new_result = vec![average];
            state
                .float_results
                .insert(destination.to_string(), new_result);
            println!("DEBUG: Average calculated and stored in float_results");
            "-- Average calculated\n".to_string()
        } else {
            println!("DEBUG: Cannot calculate average of empty result");
            "-- Error: Cannot calculate average of empty result\n".to_string()
        }
    } else {
        println!(
            "DEBUG: Source '{}' not found for average calculation",
            source
        );
        format!("-- Error: Source '{}' not found\n", source)
    }
}

fn show_tables(state: &ServerState) -> String {
    println!("Entering show_tables function"); // Debug print
    if let Some(db) = &state.current_db {
        let mut output = format!("Database: {}\n\n", db.name);
        output.push_str("| Table Name |\n");
        output.push_str("|------------|\n");

        if db.tables.is_empty() {
            output.push_str("| (none)     |\n");
        } else {
            for table_name in db.tables.keys() {
                output.push_str(&format!("| {:<10} |\n", table_name));
            }
        }

        output.push_str("\n");
        output.push_str(&format!("Total tables: {}\n", db.tables.len()));
        output
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn display_table(table_name: &str, state: &ServerState) -> String {
    println!("Entering display_table function for table: {}", table_name); // Debug print
    if let Some(db) = &state.current_db {
        if let Some(table) = db.tables.get(table_name) {
            let mut output = format!("Table: {}\n\n", table_name);

            // Calculate column widths
            let col_widths: Vec<usize> = table
                .columns
                .iter()
                .map(|col| {
                    col.name.len().max(
                        col.data
                            .iter()
                            .map(|&x| x.to_string().len())
                            .max()
                            .unwrap_or(0),
                    )
                })
                .collect();

            // Print header
            output.push_str(&format_table_row(
                &col_widths,
                &table
                    .columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>(),
            ));
            output.push_str(&format_table_separator(&col_widths));

            // Print data
            let rows = table.columns[0].data.len();
            for i in 0..rows {
                let row_data: Vec<String> = table
                    .columns
                    .iter()
                    .map(|col| col.data.get(i).map_or(String::new(), |&x| x.to_string()))
                    .collect();
                output.push_str(&format_table_row(
                    &col_widths,
                    &row_data.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                ));
            }

            output.push_str(&format_table_separator(&col_widths));
            output
        } else {
            format!("-- Error: Table '{}' not found\n", table_name)
        }
    } else {
        "-- Error: No active database\n".to_string()
    }
}

fn format_table_row(widths: &[usize], values: &[&str]) -> String {
    let cells: Vec<String> = widths
        .iter()
        .zip(values.iter())
        .map(|(&w, &v)| format!("| {:<width$} ", v, width = w))
        .collect();
    format!("{:}|\n", cells.join(""))
}

fn format_table_separator(widths: &[usize]) -> String {
    let cells: Vec<String> = widths
        .iter()
        .map(|&w| format!("+{:-<width$}", "", width = w + 2))
        .collect();
    format!("{:}+\n", cells.join(""))
}

// Serialization

const DB_FILE: &str = "database.dat";

fn save_database(state: &ServerState) -> io::Result<()> {
    let config = bincode::config::standard();
    let serialized =
        bincode::encode_to_vec(state, config).map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(DB_FILE)?;
    file.set_len(serialized.len() as u64)?;
    let mut mmap = unsafe { MmapMut::map_mut(&file)? };
    mmap.copy_from_slice(&serialized);
    mmap.flush()?;
    Ok(())
}

fn load_database() -> io::Result<ServerState> {
    let file = OpenOptions::new().read(true).open(DB_FILE)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let config = bincode::config::standard();
    let (state, _) = bincode::decode_from_slice(&mmap, config)
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
    Ok(state)
}
