use std::path::Path;
use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;

use image::GenericImage;

// Supported commands:
// send pixel: 'PX {x} {y} {GG or RRGGBB or RRGGBBAA as HEX}\n'
// set offset for future pixels: 'OFFSET {x} {y}\n'
// request pixel color: 'PX {x} {y}\n'
// request output resolution: 'SIZE\n'
// request client connection count: 'CONNECTIONS\n'
// request help message with all commands: 'HELP\n'

// production addr.
const ADDR: &str = "151.217.40.82:1234";

const WIDTH: u32 = 1920;
const HEIGHT: u32 = 1080;

fn chunkify_square(x: u32, y: u32, size: u32, rgb: u32, n: u32) -> Vec<Vec<u8>> {
    let lines_per = size / n;

    (0..n).map(|i| {
        let mut v = Vec::new();

        let line_off = i*lines_per;
        println!("Worker {} doing lines {}..{}", i, x + line_off, x + line_off + lines_per - 1);

        for line in 0..lines_per {
            for px_y in 0..size {
                write!(&mut v, "PX {} {} {:06X}\n", x + line + line_off, y + px_y, rgb)
                    .expect("greater than 64??");
            }
        }

        v
    }).collect()
}

fn chunkify_image(path: &str, n: u32) -> Vec<Vec<u8>> {
    use std::fmt::Write as WriteFmt;
    
    let mut tiny = image::open(path).unwrap().to_rgb();
    
    let w = tiny.width();
    let h = tiny.height();
    
    let mut cmds = Vec::new();
    for x in 0..w {
        for y in 0..h {
            let pixel = tiny.get_pixel(x, y);
            
            let cmd = format!("PX {} {} {:02X}{:02X}{:02X}\n", x, y, pixel.data[0], pixel.data[1], pixel.data[2]);
            cmds.push(cmd);
        }
    }
    
    let pixels_per_chunk = (w * h) / n;
    cmds.chunks(pixels_per_chunk as usize).map(|chunk| {
        let mut chunk_buf = Vec::<u8>::new();
        
        for cmd in chunk {
            chunk_buf.extend(cmd.bytes());
        }
        
        chunk_buf
    }).collect()
}

// run a worker with a programmed batch.
fn worker(chunk: &[u8]) -> io::Result<()> {
    let mut stream = BufWriter::new(TcpStream::connect(ADDR)?);

    loop {
        stream.write_all(chunk)?;
        stream.flush()?;
    }
}

fn main() {
    let mut handles = Vec::new();
    // let chunks = chunkify_square(300, 300, 700, 0xFF0000, 16);
    let chunks = chunkify_image("/Users/pepyakin/Downloads/2018-12-27 18.25.20.jpg", 16);
    for (i, chunk) in chunks.into_iter().enumerate() {
        handles.push(thread::spawn(move || loop {
            if let Err(e) = worker(&chunk[..]) {
                println!("Restarting thread {}: {:?}", i, e);   
            }
        }));
    }
    
    for handle in handles {
        handle.join();
    }
}
