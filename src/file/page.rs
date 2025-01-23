pub struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn new_from_size(blocksize: usize) -> Page {
        Page {
            bb: vec![0; blocksize],
        }
    }

    pub fn new_from_vec(b: &[u8]) -> Page {
        Page { bb: b.to_vec() }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.bb[offset..offset + 4]);
        i32::from_be_bytes(bytes)
    }

    pub fn set_int(&mut self, offset: usize, n: i32) {
        let bytes = n.to_be_bytes();
        self.bb[offset..offset + 4].copy_from_slice(&bytes);
    }

    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.get_int(offset) as usize;
        let pos = offset + 4;
        let mut bytes = vec![0u8; length];
        bytes.copy_from_slice(&self.bb[pos..pos + length]);
        return bytes;
    }

    pub fn set_bytes(&mut self, offset: usize, b: &[u8]) {
        self.set_int(offset, b.len() as i32);
        let pos = offset + 4;
        self.bb[pos..pos + b.len()].copy_from_slice(&b);
    }

    pub fn get_string(&self, offset: usize) -> Result<String, std::string::FromUtf8Error> {
        let b = self.get_bytes(offset);
        String::from_utf8(b)
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        let b = s.as_bytes();
        self.set_bytes(offset, b);
    }

    pub fn max_length(strlen: usize) -> usize {
        // utf-8 での最大長は 6 byte なはず...
        // https://stackoverflow.com/questions/9533258/what-is-the-maximum-number-of-bytes-for-a-utf-8-encoded-character
        return 4 + (strlen * 6);
    }

    pub(crate) fn contents_mut(&mut self) -> &mut Vec<u8> {
        &mut self.bb
    }

    pub(crate) fn contents(&self) -> &Vec<u8> {
        &self.bb
    }
}

#[cfg(test)]
mod test_page {
    use super::*;

    #[test]
    fn test_page() {
        let mut page = Page::new_from_size(400);

        page.set_int(0, 123);
        assert_eq!(page.get_int(0), 123);

        page.set_bytes(8, &vec![1, 2, 3, 4, 5]);
        assert_eq!(page.get_int(0), 123);
        assert_eq!(page.get_bytes(8), vec![1, 2, 3, 4, 5]);

        page.set_string(20, "hello");
        assert_eq!(page.get_int(0), 123);
        assert_eq!(page.get_bytes(8), vec![1, 2, 3, 4, 5]);
        assert_eq!(page.get_string(20).unwrap(), "hello");

        let contents = page.contents();
        assert_eq!(contents.len(), 400);
        assert_eq!(contents[0..4], vec![0, 0, 0, 123]);
        assert_eq!(contents[8..17], vec![0, 0, 0, 5, 1, 2, 3, 4, 5]);
    }
}
