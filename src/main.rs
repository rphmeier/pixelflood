use std::path::Path;
use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;
use std::sync::mpsc::{self, TryRecvError};

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

const THREADS: u32 = 20;

fn chunkify_square(x: u32, y: u32, size: u32, rgb: u32, n: u32) -> Vec<Vec<u8>> {
    let lines_per = size / n;

    (0..n).map(|i| {
        let mut v = Vec::new();

        let line_off = i*lines_per;

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

struct Work(Vec<u8>);

// run a worker with a command receiver.
fn worker(rx: &mpsc::Receiver<Work>) -> io::Result<()> {
    let mut stream = BufWriter::new(TcpStream::connect(ADDR)?);

    let mut chunk = match rx.recv() {
        Ok(Work(chunk)) => chunk,
        Err(e) => panic!("main thread never sent work: {:?}", e),
    };

    loop {
        // check for new work.
        match rx.try_recv() {
            Ok(Work(new_chunk)) => { chunk = new_chunk },
            Err(TryRecvError::Empty) => {},
            Err(_) => panic!("main thread hung up."), 
        }

        stream.write_all(&chunk[..])?;
        stream.flush()?;
    }
}

struct Worker {
    commands: mpsc::Sender<Work>,
    join_handle: thread::JoinHandle<()>,
}

fn main() {
    let mut workers = Vec::new();
    
    for i in 0..THREADS {
        let (tx, rx) = mpsc::channel();

        let join_handle = thread::spawn(move || loop {
            if let Err(e) = worker(&rx) {
                println!("Restarting thread {}: {:?}", i, e);   
            }
        });

        workers.push(Worker { commands: tx, join_handle });
    }
    
    let chunks = chunkify_image("/Users/pepyakin/Downloads/2018-12-27 18.25.20.jpg", 16);
    for (i, chunk) in chunks.into_iter().enumerate() {
        let _ = workers[i].commands.send(Work(chunk));
    }
    
    for handle in workers {
        handle.join_handle.join().unwrap();
    }
}
