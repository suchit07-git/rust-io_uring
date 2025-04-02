use io_uring::{opcode, types, IoUring};
use std::fs::OpenOptions;
use std::io;
use std::os::unix::io::AsRawFd;

const QUEUE_DEPTH: usize = 16;
const BUFFER_SIZE: usize = 4096;

struct IORequest {
    buffer: Vec<u8>,
    offset: u64,
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <input_file> <output_file>", args[0]);
        std::process::exit(1);
    }

    let mut ring = IoUring::new((QUEUE_DEPTH * 2) as u32)?;
    let input_path = &args[1];
    let output_path = &args[2];

    let input_file = OpenOptions::new().read(true).open(input_path)?;
    let input_fd = input_file.as_raw_fd();

    let output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_path)?;
    let output_fd = output_file.as_raw_fd();

    println!("Reading from: {}", input_path);
    println!("Writing to: {}", output_path);

    let mut pending_reads = Vec::new();
    let mut pending_writes = Vec::new();
    let mut total_bytes_written = 0;

    for i in 0..QUEUE_DEPTH {
        let mut buffer = vec![0u8; BUFFER_SIZE];
        let offset = (i * BUFFER_SIZE) as u64;

        let read_e = opcode::Read::new(types::Fd(input_fd), buffer.as_mut_ptr(), BUFFER_SIZE as _)
            .offset(offset)
            .build()
            .user_data(offset);

        unsafe {
            ring.submission()
                .push(&read_e)
                .expect("Failed to queue read request");
        }

        pending_reads.push(IORequest { buffer, offset });
    }

    while !pending_reads.is_empty() || !pending_writes.is_empty() {
        ring.submit_and_wait(1)?;

        let completions: Vec<_> = ring.completion().collect();
        for cqe in completions.iter() {
            let result = cqe.result();
            let user_data = cqe.user_data();

            if result < 0 {
                eprintln!("I/O error: {}", io::Error::from_raw_os_error(-result));
                continue;
            }

            if let Some(index) = pending_reads.iter().position(|r| r.offset == user_data) {
                let read_request = pending_reads.remove(index);
                if result == 0 {
                    println!("Reached end of file.");
                    continue;
                }

                let bytes_read = result as usize;

                let write_e = opcode::Write::new(
                    types::Fd(output_fd),
                    read_request.buffer.as_ptr(),
                    bytes_read as _,
                )
                .offset(read_request.offset)
                .build()
                .user_data(read_request.offset + BUFFER_SIZE as u64);

                unsafe {
                    ring.submission()
                        .push(&write_e)
                        .expect("Failed to queue write request");
                }

                pending_writes.push(IORequest {
                    buffer: read_request.buffer,
                    offset: read_request.offset,
                });
                println!("Read {} bytes at offset {}", bytes_read, user_data);
            } else if let Some(index) = pending_writes
                .iter()
                .position(|w| w.offset == user_data - BUFFER_SIZE as u64)
            {
                let write_request = pending_writes.remove(index);
                total_bytes_written += result as usize;
                println!(
                    "Written {} bytes at offset {}",
                    result, write_request.offset
                );

                let offset = write_request.offset + (QUEUE_DEPTH as u64 * BUFFER_SIZE as u64);
                let mut buffer = vec![0u8; BUFFER_SIZE];

                let read_e =
                    opcode::Read::new(types::Fd(input_fd), buffer.as_mut_ptr(), BUFFER_SIZE as _)
                        .offset(offset)
                        .build()
                        .user_data(offset);

                unsafe {
                    ring.submission()
                        .push(&read_e)
                        .expect("Failed to queue next read request");
                }

                pending_reads.push(IORequest { buffer, offset });
            }
        }
    }

    println!("Total bytes written: {}", total_bytes_written);
    Ok(())
}
