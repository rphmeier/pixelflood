#[macro_use] extern crate serde_derive;
#[macro_use] extern crate libp2p;

use std::net::SocketAddr;
use std::io::{self, Read, Write, BufWriter};
use std::thread;
use std::sync::Arc;

use futures::prelude::*;
use futures::{future, stream};
use futures::sync::mpsc;

use parking_lot::Mutex;
use libp2p::tokio_io::{self, io as async_io};

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

#[derive(Debug)]
struct ChunkDescriptor {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

fn chunks(width: u32, height: u32) -> impl Iterator<Item=ChunkDescriptor> {
    const CHUNK_DIM: u32 = 99; // 100 is 3 bytes, so we limit to 2.
    
    let col_per = width / CHUNK_DIM;
    let h_split = col_per + if width % CHUNK_DIM == 0 { 0 } else { 1 };

    let row_per = height / CHUNK_DIM;
    let v_split = row_per + if height % CHUNK_DIM == 0 { 0 } else { 1 };

    (0..h_split).flat_map(move |i| {
        let col_off = i*CHUNK_DIM;

        (0..v_split).map(move |j| {
            let row_off = j*CHUNK_DIM;
            let local_width = std::cmp::min(width - col_off, CHUNK_DIM);
            let local_height = std::cmp::min(height - row_off, CHUNK_DIM);

            ChunkDescriptor {
                x: col_off,
                y: row_off,
                width: local_width,
                height: local_height,
            }
        })
    })
}

#[derive(Serialize, Deserialize)]
struct Chunked {
    raw: Vec<u8>,
    valid_offsets: Vec<u32>,
}

/// Chunkify some rectangular thing. Provide a function for color lookup (none for alpha)
/// for any pixel within the provided width and height.
fn chunkify<F>(x: u32, y: u32, w: u32, h: u32, rgb: F) -> Chunked
    where F: Fn(u32, u32) -> Option<[u8; 3]>
{
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    for descriptor in chunks(w, h) {
        let pre_len = data.len();
        write!(
            &mut data,
            "OFFSET {} {}\n",
            x + descriptor.x,
            y + descriptor.y,
        )
            .expect("can always write to vec");

        let offset_len = data.len();

        let coords = (0..descriptor.width)
            .flat_map(|x| (0..descriptor.height).map(move |y| (x, y)));

        for (px_x, px_y) in coords {
            let pixel = match rgb(descriptor.x + px_x, descriptor.y + px_y) {
                Some(p) => p,
                None => continue,
            };

            write!(
                &mut data, 
                "PX {} {} {:02X}{:02X}{:02X}\n", 
                px_x, 
                px_y, 
                pixel[0], 
                pixel[1], 
                pixel[2],
            )
                .expect("can always write to vec");
        }

        if data.len() == offset_len {
            // empty chunk.
            data.truncate(pre_len);
        } else {
            // note valid offset.
            offsets.push(pre_len as u32);
        }
    }

    Chunked {
        raw: data,
        valid_offsets: offsets,
    }
}

/// Chunkify an RGBA image.
fn chunkify_image(x: u32, y: u32, image: &image::RgbaImage) -> Chunked {
    chunkify(x, y, image.width(), image.height(), |px_x, px_y| {
        let mut rgb = [0u8; 3];
        let pixel = image.get_pixel(px_x, px_y);

        if pixel.data[3] == 0 {
            None
        } else {
            rgb.copy_from_slice(&pixel.data[..3]);
            Some(rgb)
        }
    })
}

/// A draw command, to be sent either to a local worker or a remote connection.
#[derive(Clone)]
pub(crate) struct DrawCommand {
    data: Arc<Chunked>,
}

impl DrawCommand {
    fn chunked_data(&self) -> &Chunked { &*self.data }
    fn from_chunked(chunked: Chunked) -> Self {
        DrawCommand { data: Arc::new(chunked) }
    }

    // writes to the writer starting from a random valid point in the
    // chunked data and then looping around again.
    fn write_random<W: tokio_io::AsyncWrite>(self, write: W) -> impl Future<Item=(W, Self),Error=io::Error> {
        use rand::{self, Rng};

        struct RandomChoice {
            data: Arc<Chunked>,
            start: usize,
            end: usize,
        }

        impl AsRef<[u8]> for RandomChoice {
            fn as_ref(&self) -> &[u8] {
                &self.data.raw[self.start..self.end]
            }
        }

        let offset_i = rand::thread_rng().gen_range(0, self.data.valid_offsets.len());
        let offset = self.data.valid_offsets[offset_i];
        let len = self.data.raw.len();

        async_io::write_all(write, RandomChoice { // write offset..
            data: self.data, 
            start: offset as _, 
            end: len 
        })
            .and_then(|(write, choice)| async_io::write_all(write, RandomChoice {
                data: choice.data, // write ..offset
                start: 0,
                end: choice.start,
            }))
            .and_then(|(write, choice)| {
                let s = DrawCommand { data: choice.data }; // and flush.
                async_io::flush(write).map(move |w| (w, s))
            })
    }
}

// run a worker with a command receiver.
fn worker<'a>(screen: SocketAddr, rx: &'a mut mpsc::UnboundedReceiver<DrawCommand>) -> impl Future<Item=(),Error=io::Error> + 'a {
    let wait_for_first_work = rx.into_future()
        .map(|(c, s)| (c.expect("main thread hung up"), s))
        .map_err(|(e, _)| panic!("main thread never sent work: {:?}", e));
    
    tokio_tcp::TcpStream::connect(&screen)
        .join(wait_for_first_work)
        .and_then(move |(stream, (command, rx))| {
            println!("connected to screen and got first work.");

            if let Err(e) = stream.set_nodelay(true) {
                return future::Either::A(future::err(e));
            }

            let stream = BufWriter::new(stream);
            let loop_forever = future::loop_fn((stream, command), move |(stream, mut command)| {
                // check for new work.
                match rx.poll() {
                    Ok(Async::NotReady) => {},
                    Ok(Async::Ready(None)) | Err(()) => panic!("main thread hung up"),
                    Ok(Async::Ready(Some(new_command))) => { command = new_command },
                }

                command.write_random(stream).map(future::Loop::Continue)
            });

            future::Either::B(loop_forever)
        })
}

fn work_producer(
    mut x: u32, 
    mut y: u32, 
    image: &image::RgbaImage, 
    workers: &[mpsc::UnboundedSender<DrawCommand>],
) {
    const D_X: u32 = 25;
    const D_Y: u32 = 50;

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    loop {
        let chunks = chunkify_image(x, y, &image);
        let command = DrawCommand { data: Arc::new(chunks) };

        // relay to workers.
        for worker in workers {
            let _ = worker.unbounded_send(command.clone());
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

fn run_server(
    magic: [u8; 16], 
    new_commands: mpsc::UnboundedReceiver<DrawCommand>, 
    port: u16,
) -> impl Future<Item=(),Error=()> {
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
                let sinks = sinks.clone();
                println!("Sending work to new connection");
                Box::new(
                    sink.send(cmd.clone())
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

            last_command = Some(command.clone());

            // send to all sinks.
            // if it succeeds we add again to the buffer.
            let last_sinks = {
                let mut sinks = sinks.lock();
                std::mem::replace(&mut *sinks, Vec::new())
            };

            let work: Vec<_> = last_sinks.into_iter()
                .map(|sink| sink.send(command.clone()))
                .collect();

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
    // local and potentially remote workers.
    let mut worker_handles = Vec::new();

    let screen: SocketAddr = std::env::var("SCREEN").ok()
        .unwrap_or_else(|| BIG_SCREEN.to_string())
        .parse()
        .unwrap();

    for i in 0..THREADS {
        let (tx, mut rx) = mpsc::unbounded();

        thread::spawn(move || loop {
            if let Err(e) = worker(screen, &mut rx).wait() {
                println!("Restarting thread {}: {:?}", i, e);   
            }
        });

        worker_handles.push(tx);
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
            distributed::client(addr.parse().expect("invalid multiaddr"), magic)
                .and_then(move |work_stream| {
                    work_stream.for_each(move |work| {
                        println!("Got work with {} parts from server.", work.valid_offsets.len());
                        let command = DrawCommand::from_chunked(work);
                        for worker in &worker_handles {
                            let _ = worker.unbounded_send(command.clone());
                        }
                        Ok(())
                    })
                })
                .wait()
                .unwrap();
        }
        None => {
            // add remote workers.
            let (remote_tx, remote_rx) = mpsc::unbounded();
            worker_handles.push(remote_tx);

            let port: u16 = std::env::var("PORT").ok().map(|s| s.parse().unwrap()).unwrap_or(0);

            let img_path = std::env::var("IMG_PATH").expect("Set IMG_PATH env var");
            let image = image::open(img_path.as_str()).expect("provide valid image").to_rgba();

            std::thread::spawn(move || 
                work_producer(0, 0, &image, &worker_handles[..])
            );

            run_server(magic, remote_rx, port).wait().unwrap();
        }
    }
}
