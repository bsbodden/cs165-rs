use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

fn receive_response(stream: &mut TcpStream) -> io::Result<String> {
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer)?;

    String::from_utf8(buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() > 1 {
        // Command-line mode
        let command = &args[1];
        execute_command(command)
    } else {
        // Interactive mode
        run_interactive_mode()
    }
}

fn execute_command(command: &str) -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080")?;

    stream.write_all(command.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let response = receive_response(&mut stream)?;
    print!("{}", response);

    Ok(())
}

fn run_interactive_mode() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080")?;
    println!("Connected to server.");

    let stdin = io::stdin();
    let mut stdin_reader = stdin.lock();
    let mut input = String::new();

    loop {
        print!("db_client > ");
        io::stdout().flush()?;

        input.clear();
        stdin_reader.read_line(&mut input)?;

        let trimmed_input = input.trim();
        if trimmed_input.is_empty() {
            continue;
        }

        if trimmed_input == "quit" {
            println!("Exiting client...");
            break;
        }

        stream.write_all(input.as_bytes())?;
        stream.flush()?;

        match receive_response(&mut stream) {
            Ok(response) => {
                println!("{}", response.trim_end());
                if response.contains("Shutting down the server") {
                    println!("Server is shutting down. Exiting client...");
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error receiving response: {}", e);
                break;
            }
        }
    }

    Ok(())
}
