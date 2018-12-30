use std::mem;
use std::io::{Error as IoError, BufReader};

use futures::{future, stream, Future};
use libp2p::secio::{SecioConfig, SecioKeyPair, SecioOutput};
use libp2p::core::Multiaddr;
use libp2p::core::transport::Transport;
use libp2p::tcp::TcpConfig;
use libp2p::tokio_io::{io, AsyncRead, AsyncWrite};
use futures::prelude::*;
use byteorder::{ByteOrder, LittleEndian};
use crate::{Chunked, DrawCommand};

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

/// A sink for a command over a remote connection.
pub(crate) struct CommandSink<T: 'static> {
    state: CommandSinkState<T>,
}

enum CommandSinkState<T: 'static> {
    Ready(T),
    Writing(Box<Future<Item=T,Error=IoError>>),
    Empty,
}

impl<T: 'static> Sink for CommandSink<T> 
    where T: AsyncWrite,
{
    type SinkItem = DrawCommand;
    type SinkError = IoError;

    fn start_send(&mut self, item: DrawCommand) -> StartSend<DrawCommand, IoError> {
        match mem::replace(&mut self.state, CommandSinkState::Empty) {
            CommandSinkState::Ready(s) => {
                const MAX_BUFFER: usize = 6 * 1024 * 1024;

                let inner = item.chunked_data();
                let size = bincode::serialized_size(&inner).expect("can serialize bytes");

                let mut buf = vec![0; 4 + size as usize];
                LittleEndian::write_u32(&mut buf[..4], size as u32);
                bincode::serialize_into(&mut buf[4..], &inner).expect("can serialize bytes");

                // write while flushing periodically to avoid running into buffer issues.
                let write_chunked = future::loop_fn((s, buf), |(s, mut buf)| {
                    if buf.len() <= MAX_BUFFER {
                        let a = io::write_all(s, buf).and_then(|(s, _)| io::flush(s).map(future::Loop::Break));
                        future::Either::A(a)
                    } else {
                        let pre_buf: Vec<_> = buf.drain(..MAX_BUFFER).collect();
                        let b = io::write_all(s, pre_buf).and_then(|(s, _)| io::flush(s))
                            .map(move |s| future::Loop::Continue((s, buf)));
                        future::Either::B(b)
                    }
                });

                self.state = CommandSinkState::Writing(Box::new(write_chunked));
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
        match mem::replace(&mut self.state, CommandSinkState::Empty) {
            CommandSinkState::Writing(mut work) => match work.poll()? {
                Async::NotReady => {
                    self.state = CommandSinkState::Writing(work);
                    Ok(Async::NotReady)
                }
                Async::Ready(w) => {
                    self.state = CommandSinkState::Ready(w);
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
pub fn listen(addr: Multiaddr, magic: [u8; 16]) -> impl Stream<Item=CommandSink<impl AsyncWrite>, Error=Error> {
    listen_secure(addr)
        .map_err(Into::into)
        .and_then(move |conn| {
            let (read, write) = conn.split();
            let read = BufReader::new(read);

            io::write_all(write, magic)
                .and_then(|(w, _)| io::flush(w))
                .and_then(move |w| io::read_exact(read, [0u8; 16])
                    .map(move |(a, b)| (a, b, w))
                )
                .map_err(Into::into)
                .map(move |(_read, received_magic, write)| {
                    if received_magic != &magic[..] {
                        println!("Rejecting client: bad magic number");
                        None
                    } else {
                        println!("Handshook connection");
                        Some(CommandSink { state: CommandSinkState::Ready(write) })
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
pub fn client(addr: Multiaddr, magic: [u8; 16]) -> impl Future<Item=impl Stream<Item=Chunked,Error=Error>, Error=Error> {
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
                Ok(io::write_all(write, magic).map(move |(w, _)| (read, w)))
            }
        })
        .and_then(|write_all| write_all.map_err(Into::into))
        .and_then(|(read, write)| io::flush(write).map(move |_| read).map_err(Into::into))
        .map(|read| {
            stream::unfold(read, |read| {
                let read_next = io::read_exact(read, [0; 4])
                    .and_then(|(read, len)| {
                        let len = LittleEndian::read_u32(&len[..]);
                        let buf = vec![0; len as usize];
                        io::read_exact(read, buf)
                    })
                    .map_err(Into::into)
                    .and_then(|(read, buf)| match bincode::deserialize(&buf) {
                        Ok(data) => Ok((data, read)),
                        Err(e) => Err(Error::BadPayload(e)),
                    });

                Some(read_next)
            })
        })
}