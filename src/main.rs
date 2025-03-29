use io_uring::{opcode, types, IoUring};
use std::fs::OpenOptions;
use std::io::{self};
use std::os::unix::io::AsRawFd;

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <input_file> <output_file>", args[0]);
        std::process::exit(1);
    }
    let mut ring = IoUring::new(8)?;
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

    let mut buffer = vec![0u8; 4096];
    let mut total_bytes_read = 0;
    let mut total_bytes_written = 0;
    let mut read_offset: u64 = 0;

    loop {
        unsafe {
            let read_e =
                opcode::Read::new(types::Fd(input_fd), buffer.as_mut_ptr(), buffer.len() as _)
                    .offset(read_offset)
                    .build()
                    .user_data(0x1);

            ring.submission()
                .push(&read_e)
                .expect("Failed to push read request");
        }

        ring.submit_and_wait(1)?;
        let read_cqe = ring.completion().next().expect("Read CQE missing");
        let bytes_read = read_cqe.result();

        if bytes_read < 0 {
            eprintln!("Read error: {}", io::Error::from_raw_os_error(-bytes_read));
            break;
        }

        if bytes_read == 0 {
            println!("Reached end of file.");
            break;
        }

        total_bytes_read += bytes_read;
        read_offset += bytes_read as u64;
        println!("Read {} bytes", bytes_read);

        let mut write_offset = 0;
        let mut output_offset: u64 = total_bytes_written as u64;

        while write_offset < bytes_read {
            let bytes_to_write = bytes_read - write_offset;

            unsafe {
                let write_e = opcode::Write::new(
                    types::Fd(output_fd),
                    buffer[write_offset as usize..].as_ptr(),
                    bytes_to_write as _,
                )
                .offset(output_offset)
                .build()
                .user_data(0x2);

                ring.submission()
                    .push(&write_e)
                    .expect("Failed to push write request");
            }

            ring.submit_and_wait(1)?;
            let write_cqe = ring.completion().next().expect("Write CQE missing");
            let bytes_written = write_cqe.result();

            if bytes_written < 0 {
                eprintln!(
                    "Write error: {}",
                    io::Error::from_raw_os_error(-bytes_written)
                );
                break;
            }

            output_offset += bytes_written as u64;
            total_bytes_written += bytes_written;
            write_offset += bytes_written;
            println!("Written {} bytes", bytes_written);
        }
    }

    println!("Total bytes read: {}", total_bytes_read);
    println!("Total bytes written: {}", total_bytes_written);

    Ok(())
}
