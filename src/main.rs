//#[macro_use] extern crate serde_derive;
#[macro_use] extern crate libp2p;

use std::net::{TcpStream, SocketAddr};
use std::io::{self, Read, Write, BufWriter};
use std::thread;
use std::sync::mpsc::{self as std_mpsc, TryRecvError};
use std::sync::Arc;

use futures::prelude::*;
use futures::{future, stream};
use futures::sync::mpsc;

use parking_lot::Mutex;

mod distributed;

// Supported commands on server side:
// send pixel: 'PX {x} {y} {GG or RRGGBB or RRGGBBAA as HEX}\n'
// set offset for future pixels: 'OFFSET {x} {y}\n'
// request pixel color: 'PX {x} {y}\n'
// request output resolution: 'SIZE\n'
// request client connection count: 'CONNECTIONS\n'
// request help message with all commands: 'HELP\n'

// production addr.
const BIG_SCREEN: &str = "151.217.40.82:1234";

const THREADS: u32 = 10;

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

        write!(
            &mut v,
            "OFFSET {} {}\n",
            x,
            y
        )
            .expect("can always write to vec");

        for line in 0..descriptor.width {
            for px_y in 0..descriptor.height {
                write!(
                    &mut v, 
                    "PX {} {} {:06X}\n", 
                    descriptor.x + line,
                    descriptor.y + px_y, 
                    rgb,
                )
                    .expect("can always write to vec");
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

        write!(
            &mut v,
            "OFFSET {} {}\n",
            x,
            y
        )
            .expect("can always write to vec");

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
                    .expect("can always write to vec");
            }
        }

        v
    }).collect()
}

struct Work(Vec<u8>);

// run a worker with a command receiver.
fn worker(screen: SocketAddr, rx: &std_mpsc::Receiver<Work>) -> io::Result<()> {
    let stream = TcpStream::connect(screen)?;
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

#[derive(Clone)]
struct Worker {
    commands: std_mpsc::Sender<Work>,
}

fn with_workers(
    mut x: u32, 
    mut y: u32, 
    image: &image::RgbImage, 
    workers: &[Worker], 
    send_commands: mpsc::UnboundedSender<DrawCommand>
) {
    const D_X: u32 = 25;
    const D_Y: u32 = 50;

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    loop {
        let chunks = chunkify_image(&image, x, y, THREADS);
        let _ = send_commands.unbounded_send(DrawCommand { x, y });
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

struct DrawCommand {
    x: u32,
    y: u32,
}

fn run_server(magic: [u8; 16], image: image::RgbImage, new_commands: mpsc::UnboundedReceiver<DrawCommand>, port: u16) -> impl Future<Item=(),Error=()> {
    type BoxEmptyFut = Box<dyn Future<Item=(),Error=()>>;

    enum Event<S> {
        NewSink(S),
        Draw(DrawCommand),
    }
    
    let (sink_tx, sink_rx) = mpsc::unbounded();

    let send_sinks = distributed::listen(multiaddr![Ip4([0, 0, 0, 0]), Tcp(port)], magic)
        .for_each(move |sink| {
            sink_tx.clone().send(Event::NewSink(sink)).then(|_| Ok(()))
        })
        .map_err(|_| ());

    let sinks = Arc::new(Mutex::new(Vec::new()));
    let mut last_command: Option<DrawCommand> = None;

    let send_work = sink_rx.select(new_commands.map(Event::Draw)).for_each(move |event| match event {
        Event::NewSink(sink) => {
            if let Some(ref cmd) = last_command {
                let chunks = chunkify_image(&image, cmd.x, cmd.y, sink.n_workers());
                let sinks = sinks.clone();
                println!("Sending work to new connection");
                Box::new(
                    sink.send(distributed::WorkPackage { data: chunks })
                        .then(move |res| { match res { 
                                Ok(sink) => {
                                    sinks.lock().push(sink);
                                }
                                Err(e) => {
                                    println!("failed to send work: {:?}", e);
                                }
                            }
                            Ok(())
                        })
                ) as BoxEmptyFut
            } else {
                println!("Got new connection with no work available");
                sinks.lock().push(sink);
                Box::new(future::ok(())) as BoxEmptyFut
            }
        }
        Event::Draw(command) => {
            println!("Sending work to all connections");

            last_command = Some(command);
            let cmd = last_command.as_ref().expect("just set; qed");

            // send to all sinks.
            // if it succeeds we add again to the buffer.
            let last_sinks = {
                let mut sinks = sinks.lock();
                std::mem::replace(&mut *sinks, Vec::new())
            };

            let work: Vec<_> = last_sinks.into_iter().map(|sink| {
                let chunks = chunkify_image(&image, cmd.x, cmd.y, sink.n_workers());
                sink.send(distributed::WorkPackage { data: chunks })
            }).collect();

            let sinks = sinks.clone();
            Box::new(stream::futures_unordered(work)
                .then(move |res| match res {
                    Ok(sink) => { sinks.lock().push(sink); Ok(()) }
                    Err(e) => { println!("Connection closed: {:?}", e); Ok(()) }
                })
                .for_each(|()| Ok(()))
            ) as BoxEmptyFut
        }
    });

    send_sinks.join(send_work).then(|_| Ok(()))
}

fn main() {
    // local workers.
    let mut workers = Vec::new();

    let screen: SocketAddr = std::env::var("SCREEN").ok()
        .unwrap_or_else(|| BIG_SCREEN.to_string())
        .parse()
        .unwrap();

    for i in 0..THREADS {
        let (tx, rx) = std_mpsc::channel();

        thread::spawn(move || loop {
            if let Err(e) = worker(screen, &rx) {
                println!("Restarting thread {}: {:?}", i, e);   
            }
        });

        workers.push(Worker { commands: tx });
    }

    let magic = {
        let magic_v = std::env::var("MAGIC").ok().map(|s| s.into_bytes()).unwrap_or_else(Vec::new);
        let len = std::cmp::min(magic_v.len(), 16);
        let mut magic = [0; 16];

        magic[..len].copy_from_slice(&magic_v[0..len]);

        println!("magic = {:?}", magic);
        magic
    };

    match std::env::var("REMOTE").ok() {
        Some(addr) => {
            distributed::client(addr.parse().expect("invalid multiaddr"), magic, THREADS)
                .and_then(move |work_stream| {
                    work_stream.for_each(move |work| {
                        println!("Got work with {} parts from server.", work.data.len());
                        for (i, chunk) in work.data.into_iter().enumerate().take(workers.len()) {
                            let _ = workers[i].commands.send(Work(chunk));
                        }
                        Ok(())
                    })
                })
                .wait()
                .unwrap();
        }
        None => {
            let port: u16 = std::env::var("PORT").ok().map(|s| s.parse().unwrap()).unwrap_or(0);

            let img_path = std::env::var("IMG_PATH").expect("Set IMG_PATH env var");
            let image = image::open(img_path.as_str()).expect("provide valid image").to_rgb();
            let (cmd_tx, cmd_rx) = mpsc::unbounded();

            let workers_img = image.clone();
            std::thread::spawn(move || 
                with_workers(400, 400, &workers_img, &workers[..], cmd_tx)
            );

            run_server(magic, image, cmd_rx, port).wait().unwrap();
        }
    }
}
