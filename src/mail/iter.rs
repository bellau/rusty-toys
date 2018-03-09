use std::io::{self, BufReader, Read};
use std::iter::Peekable;
use mail::lines::Lines;

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
enum State {
    Begin,
    Body,
}

pub struct Iter<R: Read> {
    input: Peekable<Lines<BufReader<R>>>,
    state: State,
}

impl<R: Read> Iter<R> {
    /// Create a new `Iterator` from the given input.
    #[inline]
    pub fn new(input: R) -> Self {
        Iter {
            input: Lines::new(BufReader::new(input)).peekable(),
            state: State::Begin,
        }
    }
}

pub enum Entry {
    From(Vec<u8>),
    Body(Vec<u8>),
    End,
}

impl<R: Read> Iterator for Iter<R> {
    type Item = io::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        macro_rules! eof {
			($body:expr) => (
				if let Some(value) = $body {
					value
				}
				else {
					if self.state == State::Body {
						self.state = State::Begin;
						return Some(Ok(Entry::End));
					}

					return None;
				}
			);
		}

        macro_rules! try {
			($body:expr) => (
				match $body {
					Ok(value) =>
						value,

					Err(err) =>
						return Some(Err(err.into()))
				}
			);
		}

        loop {
            let (_, line) = try!(eof!(self.input.next()));

            match self.state {
                State::Begin => {
                    // Parse the beginning and return any errors.
                    self.state = State::Body;
                    return Some(Ok(Entry::From(line)));
                }

                State::Body => {
                    // If the line is empty there's a newline in the content or a new
                    // mail is beginning.
                    if line.is_empty() {
                        if let Ok((_, ref current)) = *eof!(self.input.peek()) {
                            // Try to parse the beginning, if it parses it's a new mail.
                            if current.starts_with("From ".as_bytes()) {
                                self.state = State::Begin;
                                return Some(Ok(Entry::End));
                            }
                        }
                    }

                    return Some(Ok(Entry::Body(line)));
                }
            }
        }
    }
}
