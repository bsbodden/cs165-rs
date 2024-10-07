use difference::Changeset;
use std::env;
use std::fs;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

const DATABASE_FILE: &str = "database.dat";

fn remove_database_file() {
    if let Err(e) = fs::remove_file(DATABASE_FILE) {
        if e.kind() != std::io::ErrorKind::NotFound {
            eprintln!("Warning: Failed to remove database file: {}", e);
        }
    }
}

fn start_server() -> Child {
    let server = Command::new("cargo")
        .args(&["run", "--bin", "server"])
        .spawn()
        .expect("Failed to start server");

    thread::sleep(Duration::from_secs(2)); // Wait for server to start

    server
}

fn run_client_command(command: &str) -> Result<String, String> {
    let output = Command::new("cargo")
        .args(&["run", "--bin", "client", command])
        .output()
        .map_err(|e| format!("Failed to execute client command: {}", e))?;

    if !output.status.success() {
        Err(format!(
            "Client command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    } else {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

fn run_test(test_number: usize, server_wait: u64) -> Result<(), String> {
    let dsl_file = format!("tests/dsl/test{:02}gen.dsl", test_number);
    let exp_file = format!("tests/dsl/test{:02}gen.exp", test_number);

    let dsl_content = std::fs::read_to_string(&dsl_file)
        .map_err(|e| format!("Failed to read DSL file: {}", e))?;
    let expected_output = std::fs::read_to_string(&exp_file)
        .map_err(|e| format!("Failed to read EXP file: {}", e))?;

    let mut actual_output = String::new();

    for line in dsl_content.lines() {
        let trimmed_line = line.trim();
        if trimmed_line.is_empty() || trimmed_line.starts_with("--") {
            continue;
        }

        match run_client_command(trimmed_line) {
            Ok(response) => {
                if trimmed_line.starts_with("print(") {
                    actual_output.push_str(&response);
                }
            }
            Err(e) => return Err(format!("Command '{}' failed: {}", trimmed_line, e)),
        }

        thread::sleep(Duration::from_millis(server_wait));
    }

    if actual_output.trim_end() != expected_output.trim_end() {
        let changeset = Changeset::new(expected_output.trim_end(), actual_output.trim_end(), "\n");
        Err(format!("Test {} failed. Diff:\n{}", test_number, changeset))
    } else {
        Ok(())
    }
}

#[test]
fn milestone_1_tests() {
    // Remove the database file at the beginning of the test
    remove_database_file();

    let upto: usize = env::var("UPTO")
        .unwrap_or_else(|_| "9".to_string())
        .parse()
        .expect("UPTO must be a positive integer");

    let server_wait: u64 = env::var("SERVER_WAIT")
        .unwrap_or_else(|_| "100".to_string())
        .parse()
        .expect("SERVER_WAIT must be a positive integer");

    let mut passed_tests = 0;
    let mut failed_tests = Vec::new();

    for test_number in 1..=upto {
        println!("Running test {}", test_number);

        // Start server for each test
        let mut server = start_server();

        // Add a delay to ensure server is ready
        thread::sleep(Duration::from_secs(2));

        match run_test(test_number, server_wait) {
            Ok(_) => {
                println!("Test {} passed", test_number);
                passed_tests += 1;
            }
            Err(e) => {
                println!("{}", e);
                failed_tests.push(test_number);
            }
        }

        // Shutdown server after each test
        let _ = run_client_command("shutdown");
        server.wait().expect("Failed to wait for server");

        // Add a delay to ensure server has fully shut down
        thread::sleep(Duration::from_secs(1));
    }

    println!("Tests passed: {}", passed_tests);
    println!("Tests failed: {}", failed_tests.len());

    if !failed_tests.is_empty() {
        panic!("Tests failed: {:?}", failed_tests);
    }
}