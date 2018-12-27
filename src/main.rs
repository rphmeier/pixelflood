use std::path::Path;
use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;
use std::sync::mpsc::{self, TryRecvError};

use image::{GenericImage, GenericImageView};

// Supported commands:
// send pixel: 'PX {x} {y} {GG or RRGGBB or RRGGBBAA as HEX}\n'
// set offset for future pixels: 'OFFSET {x} {y}\n'
// request pixel color: 'PX {x} {y}\n'
// request output resolution: 'SIZE\n'
// request client connection count: 'CONNECTIONS\n'
// request help message with all commands: 'HELP\n'

// production addr.
const BIG_SCREEN: &str = "151.217.40.82:1234";
const SMALL_SCREEN: &str = "151.217.177.136:1234";

const WIDTH: u32 = 1920;
const HEIGHT: u32 = 1080;

const THREADS: u32 = 20;

struct ChunkDescriptor {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

fn chunks(width: u32, height: u32, n: u32) -> impl Iterator<Item=ChunkDescriptor> {
    let lines_per = width / n;

    (0..n).map(move |i| {
        let line_off = i*lines_per;

        ChunkDescriptor {
            x: line_off,
            y: height,
            width: lines_per,
            height,
        }
    })
}

fn chunkify_square(x: u32, y: u32, size: u32, rgb: u32, n: u32) -> Vec<Vec<u8>> {
    let lines_per = size / n;

    chunks(size, size, n).map(|descriptor| {
        let mut v = Vec::new();

        for line in 0..descriptor.width {
            for px_y in 0..descriptor.height {
                write!(
                    &mut v, 
                    "PX {} {} {:06X}\n", 
                    descriptor.x + line,
                    descriptor.y + px_y, 
                    rgb,
                )
                    .expect("greater than 64??");
            }
        }

        v
    }).collect()
}

fn chunkify_image(x: u32, y: u32, path: &str, n: u32) -> Vec<Vec<u8>> {
    use std::fmt::Write as WriteFmt;
    
    let mut tiny = image::open(path).expect("provide valid image");
    
    tiny = tiny.resize(tiny.width() / 2, tiny.height() / 2, image::FilterType::Nearest);

    let tiny = tiny.to_rgb();

    let w = tiny.width();
    let h = tiny.height();

    chunks(w, h, n).map(|descriptor| {
        let mut v = Vec::new();

        for line in 0..descriptor.width {
            for px_y in 0..descriptor.height {
                let pixel = tiny.get_pixel(line, px_y);

                write!(
                    &mut v, 
                    "PX {} {} {:02X}{:02X}{:02X}\n", 
                    descriptor.x + line, 
                    descriptor.y + px_y, 
                    pixel.data[0], 
                    pixel.data[1], 
                    pixel.data[2],
                )
                    .expect("greater than 64??");
            }
        }

        v
    }).collect()
}

struct Work(Vec<u8>);

// run a worker with a command receiver.
fn worker(rx: &mpsc::Receiver<Work>) -> io::Result<()> {
    let mut stream = BufWriter::new(TcpStream::connect(SMALL_SCREEN)?);

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

fn with_workers(mut x: u32, mut y: u32, img_path: &str, workers: &[Worker]) {
    const D_X: u32 = 25;
    const D_Y: u32 = 50;

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    loop {
        let chunks = chunkify_image(x, y, img_path, THREADS);
        for (i, chunk) in chunks.into_iter().enumerate().take(THREADS as _) {
            let _ = workers[i].commands.send(Work(chunk));
        }

        let mut buf = [0u8];
        loop {
            stdin.read_exact(&mut buf[..]).expect("stdin closed");
            match buf[0] {
                b'w' => { y -= D_Y },
                b'a' => { x -= D_X },
                b's' => { y += D_Y },
                b'd' => { x += D_X },
                _ => continue,
            }

            break
        }
    }
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

    let img_path = std::env::var("IMG_PATH").expect("Set IMG_PATH env var");
    with_workers(400, 400, img_path.as_str(), &workers[..]);
    
    for handle in workers {
        handle.join_handle.join().unwrap();
    }
}
