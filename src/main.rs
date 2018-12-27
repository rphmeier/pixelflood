use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::thread;

// production addr.
const ADDR: &str = "151.217.40.82:1234";

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

    fn set_offset(&mut self, x: u32, y: u32) {
        let mut slice = [0; 32];
        let len = {
            let mut wrapped = WrappedSlice { inner: &mut slice, len: 0 };
            write!(wrapped, "OFFSET {} {}\n", x, y).expect("greater than 32??");
            wrapped.len
        };

        self.output.write(&slice[0..len]).expect("could not write");
    }

    fn set_pixel(&mut self, x: u32, y: u32, rgb: u32) {
        let mut slice = [0; 64];
        let len = {
            let mut wrapped = WrappedSlice { inner: &mut slice, len: 0 };
            write!(wrapped, "PX {} {} {:06X}\n", x, y, rgb).expect("greater than 64??");
            wrapped.len
        };

        self.output.write(&slice[0..len]).expect("could not write");
    }

    fn flush(&mut self) {
        self.output.flush().unwrap();
    }
}

fn flooder(offset: u32) {
    let mut stream = TcpStream::connect(ADDR).unwrap();
    let mut remote = Remote::new(&stream);

    loop {
        for i in 0..100 {
            for j in 0..100 {
                remote.set_pixel(off + i, off + j, 0x00FF00);
            }
        }

        remote.flush();
    }
}

fn main() {
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(thread::spawn(|| {
            flooder(100);
        }));
    }
    
    for handle in handles {
        handle.join();
    }
}
