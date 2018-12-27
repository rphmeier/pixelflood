use std::net::TcpStream;
use std::io::{Read, Write, BufReader, BufWriter};

// production addr.
const ADDR: &str = "151.217.40.82:1234";

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

    fn set_pixel(&mut self, x: u32, y: u32, rgb: u32) {
        let out = format!("PX {} {} {:06X}\n", x, y, rgb);

        self.output.write(out.as_bytes()).expect("could not write");
    }

    fn flush(&mut self) {
        self.output.flush().unwrap();
    }
}

fn main() {
    let mut stream = TcpStream::connect(ADDR).unwrap();
    let mut remote = Remote::new(&stream);

    loop {
        for i in 0..100 {
            for j in 0..100 {
                remote.set_pixel(i, j, 0x00FF00);
            }
            remote.flush();
        }
    }
}
