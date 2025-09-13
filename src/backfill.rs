use crate::ExportPage;
use std::io::Write;

pub struct PageForwarder<const N: usize> {
    newlines: usize,
    bytes: Vec<u8>,
    dest: flume::Sender<ExportPage>,
}

impl<const N: usize> PageForwarder<N> {
    pub fn new(dest: flume::Sender<ExportPage>) -> Self {
        Self {
            newlines: 0,
            bytes: Vec::new(),
            dest,
        }
    }
    fn send_page(&mut self) {
        log::info!("sending page!");
        let page_bytes = std::mem::take(&mut self.bytes);
        if !page_bytes.is_empty() {
            let ops = String::from_utf8(page_bytes)
                .unwrap()
                .trim()
                .replace("}{", "}\n{"); // HACK because oops the exports i made are corrupted
            self.dest.send(ExportPage { ops }).unwrap();
            self.newlines = 0;
        }
    }
}

impl<const N: usize> Write for PageForwarder<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buf = buf;
        loop {
            let newlines_to_next_split = N - 1 - self.newlines;
            let Some((i, _)) = buf
                .iter()
                .enumerate()
                .filter(|&(_, &b)| b == b'\n')
                .nth(newlines_to_next_split)
            else {
                // we're left with a partial page
                self.bytes.extend_from_slice(buf);
                // i guess we need this second pass to update the count
                self.newlines += buf.iter().filter(|&&b| b == b'\n').count();
                // could probably do it all in one pass but whatever
                break;
            };
            // we have one complete page from current bytes + buf[..i]
            let (page_rest, rest) = buf.split_at(i);
            self.bytes.extend_from_slice(page_rest);
            self.send_page();
            buf = rest;
        }

        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.send_page();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_page_forwarder_empty_flush() {
        let (tx, rx) = flume::bounded(1);
        let mut pf = PageForwarder::<1>::new(tx);
        pf.flush().unwrap();
        assert!(rx.is_empty());
    }
}
