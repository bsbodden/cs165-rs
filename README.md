# CS165 Fall 2024

## Introduction

This repository contains a Rust version of the project for CS165 Fall 2024.
More details about the project: http://daslab.seas.harvard.edu/classes/cs165/project.html

## Building

Build the project (both client and server):

```bash
cargo build
```

## Running the Server

Launch the server:

```bash
cargo run --bin server
```

## Running the Client

Launch the client:

```bash
cargo run --bin client
```

## Running the Tests for Milestone 1

Run the test for milestone one up to test #2 and
with a two-second server wait between tests:

```bash
UPTO=2 SERVER_WAIT=2 cargo test --test test_milestone_1
```