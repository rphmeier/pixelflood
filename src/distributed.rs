use std::mem;
use std::io::{Error as IoError, BufReader};

use futures::{future, stream, Future};
use libp2p::secio::{SecioConfig, SecioKeyPair, SecioOutput};
use libp2p::core::{Multiaddr, upgrade::apply_inbound};
use libp2p::core::transport::Transport;
use libp2p::tcp::TcpConfig;
use libp2p::tokio_io::{io, AsyncRead, AsyncWrite};
use futures::prelude::*;
use byteorder::{ByteOrder, LittleEndian};

fn dial_secure(addr: Multiaddr) -> impl Future<Item = impl AsyncRead + AsyncWrite,Error=IoError> {
    let transport = TcpConfig::new()
        .with_upgrade({
            let keypair = SecioKeyPair::ed25519_generated().unwrap();
            SecioConfig::new(keypair)
        })
        .map(|out: SecioOutput<_>, _| out.stream);

    transport.dial(addr)
        .map_err(|_| "could not dial")
        .unwrap()
}

fn listen_secure(addr: Multiaddr) -> impl Stream<Item = impl AsyncRead + AsyncWrite, Error=IoError> {
    let transport = TcpConfig::new()
        .with_upgrade({
            let keypair = SecioKeyPair::ed25519_generated().unwrap();
            SecioConfig::new(keypair)
        })
        .map(|out: SecioOutput<_>, _| out.stream);

    let (listener, addr) = transport.listen_on(addr)
        .map_err(|_| "could not listen")
        .unwrap();

    println!("Listening on {:?}", addr);

    listener.and_then(|(upgrade, multiaddr)| {
        println!("connection from {:?}", multiaddr);
        upgrade
    })
}

/// Vector of draw commands.
pub struct WorkPackage {
    data: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub enum Error {
    Io(IoError),
    BadMagic,
    BadPayload(bincode::Error),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

/// A sink for a buffer over a remote connection.
pub struct BufSink<T> {
    n_workers: u32,
    state: BufSinkState<T>,
}

enum BufSinkState<T> {
    Ready(T),
    Writing(io::WriteAll<T, Vec<u8>>),
    Empty,
}

impl<T> BufSink<T> {
    /// The number of workers.
    pub fn n_workers(&self) -> u32 { self.n_workers }
}

impl<T> Sink for BufSink<T> 
    where T: AsyncWrite,
{
    type SinkItem = WorkPackage;
    type SinkError = IoError;

    fn start_send(&mut self, item: WorkPackage) -> StartSend<WorkPackage, IoError> {
        match mem::replace(&mut self.state, BufSinkState::Empty) {
            BufSinkState::Ready(s) => {
                let size = bincode::serialized_size(&item.data).expect("can serialize bytes");

                let mut buf = vec![0; 4 + size as usize];
                LittleEndian::write_u32(&mut buf[..4], size as u32);
                bincode::serialize_into(&mut buf[4..], &item.data).expect("can serialize bytes");

                self.state = BufSinkState::Writing(io::write_all(s, buf));
                Ok(AsyncSink::Ready)
            }
            other => {
                self.state = other;
                
                match self.poll_complete()? {
                    Async::Ready(()) => self.start_send(item),
                    Async::NotReady => Ok(AsyncSink::NotReady(item)),
                }
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), IoError> {
        match mem::replace(&mut self.state, BufSinkState::Empty) {
            BufSinkState::Writing(mut work) => match work.poll()? {
                Async::NotReady => {
                    self.state = BufSinkState::Writing(work);
                    Ok(Async::NotReady)
                }
                Async::Ready((w, _)) => {
                    self.state = BufSinkState::Ready(w);
                    Ok(Async::Ready(()))
                }
            }
            other => {
                self.state = other;
                Ok(Async::NotReady)
            }
        }
    }

    fn close(&mut self) -> Poll<(), IoError> {
        self.poll_complete()
    }
}

/// Listen on server.
pub fn listen(addr: Multiaddr, magic: [u8; 16]) -> impl Stream<Item=BufSink<impl AsyncWrite>, Error=Error> {
    listen_secure(addr)
        .map_err(Into::into)
        .and_then(move |conn| {
            let (read, write) = conn.split();
            let read = BufReader::new(read);

            io::write_all(write, magic)
                .and_then(|(w, _)| io::flush(w))
                .and_then(move |w| io::read_exact(read, [0u8; 20])
                    .map(move |(a, b)| (a, b, w))
                )
                .map_err(Into::into)
                .map(move |(_read, handshake, write)| {
                    let received_magic = &handshake[0..16];
                    let n_workers = LittleEndian::read_u32(&handshake[16..]);

                    if received_magic != &magic[..] {
                        println!("Rejecting client: bad magic number");
                        None
                    } else {
                        println!("Handshook connection with {} workers", n_workers);
                        Some(BufSink { n_workers, state: BufSinkState::Ready(write) })
                    }
                })
                .then(|res: Result<_, Error>| match res {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        println!("Failed connection: {:?}", e);
                        Ok(None)
                    }
                })
        })
        .filter_map(|x| x)
}

/// Connect to server as client. Provide given magic number to compare against the server.
pub fn client(addr: Multiaddr, magic: [u8; 16], n_workers: u32) -> impl Future<Item=impl Stream<Item=WorkPackage,Error=Error>, Error=Error> {
    dial_secure(addr)
        .map(|conn| conn.split())
        .map(|(read, write)| (BufReader::new(read), write)) 
        .and_then(|(read, write)| io::read_exact(read, [0u8; 16])
            .map(move |(a, b)| (a, b, write))
        )
        .map_err(Into::into)
        .and_then(move |(read, received_magic, write)| {
            if received_magic != magic {
                println!("Mismatch: server had different magic number");
                Err(Error::BadMagic)
            } else {
                let mut handshake = [0u8; 20];
                handshake[0..16].copy_from_slice(&magic);
                LittleEndian::write_u32(&mut handshake[16..20], n_workers);
                Ok(io::write_all(write, handshake).map(move |(w, _)| (read, w)))
            }
        })
        .and_then(|write_all| write_all.map_err(Into::into))
        .and_then(|(read, write)| io::flush(write).map(move |_| read).map_err(Into::into))
        .map(|read| {
            println!("wrote all");
            stream::unfold(read, |read| {
                let read_next = io::read_exact(read, [0; 4])
                    .and_then(|(read, len)| {
                        let len = LittleEndian::read_u32(&len[..]);
                        let mut buf = vec![0; len as usize];
                        io::read_exact(read, buf)
                    })
                    .map_err(Into::into)
                    .and_then(|(read, buf)| match bincode::deserialize(&buf) {
                        Ok(data) => Ok((WorkPackage { data }, read)),
                        Err(e) => Err(Error::BadPayload(e)),
                    });

                Some(read_next)
            })
        })
}