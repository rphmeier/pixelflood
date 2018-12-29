#[macro_use] extern crate serde_derive;
#[macro_use] extern crate libp2p;

use std::path::Path;
use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;
use std::sync::mpsc::{self, TryRecvError};

use futures::prelude::*;

use image::{GenericImage, GenericImageView};

mod distributed;

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

#[allow(unused)]
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

fn chunkify_image(image: &image::RgbImage, x: u32, y: u32, n: u32) -> Vec<Vec<u8>> {
    let w = image.width();
    let h = image.height();

    chunks(w, h, n).map(|descriptor| {
        let mut v = Vec::new();

        for line in 0..descriptor.width {
            for px_y in 0..descriptor.height {
                let pixel = image.get_pixel(line, px_y);

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
    let mut stream = TcpStream::connect(BIG_SCREEN)?;
    stream.set_nonblocking(true)?;
    stream.set_nodelay(true)?;

    let mut stream = BufWriter::new(stream);

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

        if let Err(e) = stream.write_all(&chunk[..]) {
            match e.kind() {
                io::ErrorKind::WouldBlock => {},
                _ => return Err(e),
            }
        }

        if let Err(e) = stream.flush() {
            match e.kind() {
                io::ErrorKind::WouldBlock => {}
                _ => return Err(e),
            }
        }
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

    let image = image::open(img_path).expect("provide valid image").to_rgb();

    loop {
        let chunks = chunkify_image(&image, x, y, THREADS);
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

fn run_server() {
    // start distributed task.
    std::thread::spawn(|| {
        use futures::prelude::*;

        distributed::listen(multiaddr![Ip4([0, 0, 0, 0]), Tcp(0u16)], *b"deadbeefcafebabe")
            .for_each(|sink| { std::mem::forget(sink); Ok(()) })
            .wait()
            .unwrap()
    });
}

fn main() {
    // local workers.
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

    match std::env::var("REMOTE").ok() {
        Some(addr) => {
            let magic_v = std::env::var("MAGIC").ok().map(|s| s.into_bytes()).unwrap_or_else(Vec::new);
            let len = std::cmp::min(magic_v.len(), 16);
            let mut magic = [0; 16];
            magic.copy_from_slice(&magic_v[..len]);

            distributed::client(addr.parse().expect("invalid multiaddr"), magic, THREADS)
                .and_then(move |work_stream| {
                    work_stream.for_each(move |work| {
                        drop(work);
                        Ok(())
                    })
                })
                .wait()
                .unwrap();
        }
        None => {
            run_server();
            let img_path = std::env::var("IMG_PATH").expect("Set IMG_PATH env var");
            with_workers(400, 400, img_path.as_str(), &workers[..]);
        }
    }

    for handle in workers {
        handle.join_handle.join().unwrap();
    }
}
