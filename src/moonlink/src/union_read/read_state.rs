// A read state is a collection of objects that are shared between moonlink and readers
//
// Meant to be sent using either shared memory or network connection.
//

#[derive(Debug)]
pub struct ReadState {
    pub files: Vec<String>,
    pub deletions: Vec<(u32, u32)>,
}

impl Drop for ReadState {
    fn drop(&mut self) {
        println!("Dropping read state");
    }
}

impl ReadState {
    pub(super) fn new(input: (Vec<String>, Vec<(u32, u32)>)) -> Self {
        ReadState {
            files: input.0,
            deletions: input.1,
        }
    }
}
