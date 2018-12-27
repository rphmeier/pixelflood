use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;

// production addr.
const ADDR: &str = "151.217.40.82:1234";

const WIDTH: u32 = 1920;
const HEIGHT: u32 = 1080;

struct WrappedSlice<'a> {
    inner: &'a mut [u8],
    len: usize,
}

impl<'a> Write for WrappedSlice<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.inner.write(buf)?;
        self.len += size;
        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct Remote<'a> {
    output: BufWriter<&'a TcpStream>,
    input: BufReader<&'a TcpStream>,
}

// Supported commands:
// send pixel: 'PX {x} {y} {GG or RRGGBB or RRGGBBAA as HEX}\n'
// set offset for future pixels: 'OFFSET {x} {y}\n'
// request pixel color: 'PX {x} {y}\n'
// request output resolution: 'SIZE\n'
// request client connection count: 'CONNECTIONS\n'
// request help message with all commands: 'HELP\n'
impl<'a> Remote<'a> {
    fn new(stream: &'a TcpStream) -> Self {
        Remote {
            output: BufWriter::new(stream),
            input: BufReader::new(stream),
        }
    }

    fn set_offset(&mut self, x: u32, y: u32) -> io::Result<()> {
        let mut slice = [0; 32];
        let len = {
            let mut wrapped = WrappedSlice { inner: &mut slice, len: 0 };
            write!(wrapped, "OFFSET {} {}\n", x, y).expect("greater than 32??");
            wrapped.len
        };

        self.output.write(&slice[0..len]).map(|_| ())
    }

    fn set_pixel(&mut self, x: u32, y: u32, rgb: u32) -> io::Result<()> {
        let mut slice = [0; 64];
        let len = {
            let mut wrapped = WrappedSlice { inner: &mut slice, len: 0 };
            write!(wrapped, "PX {} {} {:06X}\n", x, y, rgb).expect("greater than 64??");
            wrapped.len
        };

        self.output.write(&slice[0..len]).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn flooder(off_x: u32, off_y: u32) -> io::Result<()> {
    let mut stream = TcpStream::connect(ADDR)?;
    let mut remote = Remote::new(&stream);

    loop {
        for i in 0..100 {
            for j in 0..100 {
                remote.set_pixel(off_x + i, off_y + j, 0x00FF00)?;
            }
        }

        remote.flush()?;
    }
}

fn main() {
    let mut handles = Vec::new();
    for i in 0..16 {
        handles.push(thread::spawn(move || loop {
            if let Err(e) = flooder(i * 100, (i * 100) % 1080) {
                println!("Restarting thread {}: {:?}", i, e);   
            }
        }));
    }
    
    for handle in handles {
        handle.join();
    }
}
