use std::fmt::Display;

pub const VAR_CHAR_CAPACITY: usize = 32;

#[derive(Clone, Eq, PartialEq)]
pub struct VarChar {
    length: u8,
    data: [char; VAR_CHAR_CAPACITY],
}

#[derive(Debug)]
pub struct StringTooLong;

impl VarChar {
    pub fn as_slice(&self) -> &[char] {
        &self.data[0..self.length as usize]
    }

    pub fn as_bytes(&self) -> [u8; VAR_CHAR_CAPACITY * std::mem::size_of::<char>()] {
        let mut copy = self.data;
        copy[self.length as usize..].fill(char::default());
        unsafe { std::mem::transmute(copy) }
    }
}

impl Display for VarChar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            String::from_iter(self.data.iter().take(self.length as usize))
        )
    }
}

impl std::fmt::Debug for VarChar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VarChar {{ length: {}, data: \"{}\" }}",
            self.length,
            String::from_iter(self.data.iter().take(self.length as usize))
        )
    }
}

impl TryFrom<&str> for VarChar {
    type Error = StringTooLong;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() > VAR_CHAR_CAPACITY {
            return Err(StringTooLong);
        }
        let length = value.len() as u8;

        let mut data = [char::default(); VAR_CHAR_CAPACITY];
        for (ch, dest) in value.chars().zip(data.iter_mut()) {
            *dest = ch;
        }
        Ok(Self { length, data })
    }
}
