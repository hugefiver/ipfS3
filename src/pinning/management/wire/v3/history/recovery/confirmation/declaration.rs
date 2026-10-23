//! Classifies a declaration only after a syntactically valid JSON prefix.
//! Opaque staging headers have no JSON contract. After the top-level format
//! string declares the protocol prefix, strict confirmation verification owns
//! every remaining byte, including an invalid or overlong suffix.
use super::*;
mod stack;
use stack::Stack;

const OBJECT_FIRST: u8 = 0;
const OBJECT_KEY: u8 = 1;
const OBJECT_COLON: u8 = 2;
const OBJECT_VALUE: u8 = 3;
const OBJECT_AFTER: u8 = 4;
const ARRAY_FIRST: u8 = 5;
const ARRAY_VALUE: u8 = 6;
const ARRAY_AFTER: u8 = 7;
const PROTOCOL: &[u8] = b"S75-confirm-";

#[derive(Clone, Copy)]
enum Role { Key, Value }
enum Token {
    None,
    String(Text),
    Literal { expected: &'static [u8], next: usize },
    Number(Number),
}
#[derive(Clone, Copy)]
enum Number { Sign, Zero, Integer, Dot, Fraction, Exponent, ExponentSign, ExponentDigits }
impl Number {
    /// None finishes a number; Err means it was not a JSON number at all.
    fn byte(self, byte: u8) -> std::result::Result<Option<Self>, ()> {
        use Number::*;
        match (self,byte) {
            (Sign,b'0') => Ok(Some(Zero)),
            (Sign,b'1'..=b'9') => Ok(Some(Integer)),
            (Integer,b'0'..=b'9') => Ok(Some(Integer)),
            (Zero | Integer,b'.') => Ok(Some(Dot)),
            (Dot | Fraction,b'0'..=b'9') => Ok(Some(Fraction)),
            (Zero | Integer | Fraction,b'e' | b'E') => Ok(Some(Exponent)),
            (Exponent,b'+' | b'-') => Ok(Some(ExponentSign)),
            (Exponent | ExponentSign | ExponentDigits,b'0'..=b'9') => Ok(Some(ExponentDigits)),
            (Zero | Integer | Fraction | ExponentDigits,b' ' | b'\t' | b'\r' | b'\n' | b',' | b'}' | b']') => Ok(None),
            _ => Err(()),
        }
    }
}

struct Text {
    role: Role,
    target: &'static [u8],
    matched: Option<usize>,
    escaped: bool,
    unicode: Option<(u16,u8)>,
    high: Option<u16>,
    low_start: u8,
    utf8: [u8;4],
    utf8_len: usize,
    utf8_need: usize,
}
enum TextStep { More, End, Declared, Invalid }
impl Text {
    fn new(role: Role, target: &'static [u8]) -> Self {
        Self {role,target,matched:(!target.is_empty()).then_some(0),escaped:false,unicode:None,
            high:None,low_start:0,utf8:[0;4],utf8_len:0,utf8_need:0}
    }
    fn decoded(&mut self, ch: char) -> TextStep {
        if let Some(position) = self.matched {
            if ch.is_ascii() && self.target.get(position) == Some(&(ch as u8)) {
                self.matched = Some(position + 1);
                if matches!(self.role,Role::Value) && position + 1 == self.target.len() {
                    return TextStep::Declared;
                }
            } else {
                self.matched = None;
            }
        }
        TextStep::More
    }
    fn byte(&mut self, byte: u8) -> TextStep {
        if self.utf8_need != 0 {
            if !(0x80..=0xbf).contains(&byte) { return TextStep::Invalid; }
            self.utf8[self.utf8_len] = byte;
            self.utf8_len += 1;
            if self.utf8_len != self.utf8_need { return TextStep::More; }
            let ch = match std::str::from_utf8(&self.utf8[..self.utf8_len]) {
                Ok(value) => value.chars().next().expect("nonempty UTF-8 scalar"),
                Err(_) => return TextStep::Invalid,
            };
            self.utf8_need = 0;
            return self.decoded(ch);
        }
        if self.low_start != 0 {
            let expected = if self.low_start == 1 {b'\\'} else {b'u'};
            if byte != expected { return TextStep::Invalid; }
            self.low_start += 1;
            if self.low_start == 3 {self.low_start=0;self.unicode=Some((0,0));}
            return TextStep::More;
        }
        if let Some((value,digits)) = self.unicode {
            let hex = match byte { b'0'..=b'9'=>byte-b'0',b'a'..=b'f'=>byte-b'a'+10,b'A'..=b'F'=>byte-b'A'+10,_=>return TextStep::Invalid };
            let value = value * 16 + u16::from(hex);
            if digits != 3 {self.unicode=Some((value,digits+1));return TextStep::More;}
            self.unicode=None;
            if let Some(high) = self.high.take() {
                if !(0xdc00..=0xdfff).contains(&value) {return TextStep::Invalid;}
                let scalar = 0x10000 + ((u32::from(high)-0xd800)<<10) + u32::from(value)-0xdc00;
                return self.decoded(char::from_u32(scalar).expect("surrogate pair"));
            }
            if (0xd800..=0xdbff).contains(&value) {self.high=Some(value);self.low_start=1;return TextStep::More;}
            return match char::from_u32(u32::from(value)) {Some(ch)=>self.decoded(ch),None=>TextStep::Invalid};
        }
        if self.escaped {
            self.escaped=false;
            let ch = match byte {
                b'"'=>'"',b'\\'=>'\\',b'/'=>'/',b'b'=>'\u{8}',b'f'=>'\u{c}',b'n'=>'\n',b'r'=>'\r',b't'=>'\t',
                b'u'=>{self.unicode=Some((0,0));return TextStep::More;},
                _=>return TextStep::Invalid,
            };
            return self.decoded(ch);
        }
        match byte {
            b'"'=>TextStep::End,
            b'\\'=>{self.escaped=true;TextStep::More},
            0..=0x1f=>TextStep::Invalid,
            0x20..=0x7f=>self.decoded(char::from(byte)),
            0xc2..=0xf4=>{
                self.utf8[0]=byte;self.utf8_len=1;
                self.utf8_need=if byte<0xe0 {2} else if byte<0xf0 {3} else {4};
                TextStep::More
            },
            _=>TextStep::Invalid,
        }
    }
}

pub(super) struct Probe {
    stack: Stack,
    token: Token,
    started: bool,
    format_key: bool,
    result: Option<bool>,
}
impl Probe {
    pub fn new() -> Self {
        Self {stack:Stack::new(),token:Token::None,started:false,format_key:false,result:None}
    }
    pub fn feed(&mut self, bytes: &[u8], budget: &mut Budget) -> Result<Option<bool>> {
        budget.charge(0,0,0,bytes.len() as u64).map_err(storage)?;
        for (index,byte) in bytes.iter().enumerate() {
            if self.result.is_some() {break;}
            if index % 4096 == 0 {budget.check().map_err(storage)?;}
            self.byte(*byte,budget)?;
        }
        Ok(self.result)
    }
    fn byte(&mut self, byte: u8, budget: &mut Budget) -> Result<()> {
        match &mut self.token {
            Token::String(text) => {
                match text.byte(byte) {
                    TextStep::Declared=>self.result=Some(true),
                    TextStep::Invalid=>self.result=Some(false),
                    TextStep::More=>{},
                    TextStep::End=>{
                        if matches!(text.role,Role::Key) {
                            if self.stack.len()==1 {self.format_key=text.matched==Some(6);}
                            self.stack.set(OBJECT_COLON)?;
                        }
                        self.token=Token::None;
                    },
                }
                return Ok(());
            },
            Token::Literal {expected,next}=>{
                if expected.get(*next)!=Some(&byte) {self.result=Some(false);return Ok(());}
                *next+=1;
                if *next==expected.len() {self.token=Token::None;}
                return Ok(());
            },
            Token::Number(number)=>match number.byte(byte) {
                Ok(Some(next))=>{*number=next;return Ok(());},
                Err(())=>{self.result=Some(false);return Ok(());},
                Ok(None)=>self.token=Token::None,
            },
            Token::None=>{},
        }
        if matches!(byte,b' '|b'\t'|b'\r'|b'\n') {return Ok(());}
        if !self.started {
            self.started=true;
            if byte==b'{' {self.stack.push(OBJECT_FIRST,budget)?;} else {self.result=Some(false);}
            return Ok(());
        }
        match (self.stack.top()?,byte) {
            (OBJECT_FIRST,b'}') | (OBJECT_AFTER,b'}') | (ARRAY_FIRST,b']') | (ARRAY_AFTER,b']')=>{
                self.stack.pop(budget)?;
                if self.stack.len()==0 {self.result=Some(false);}
            },
            (OBJECT_FIRST | OBJECT_KEY,b'"')=>{
                let target: &'static [u8] = if self.stack.len()==1 {b"format"} else {b""};
                self.token=Token::String(Text::new(Role::Key,target));
            },
            (OBJECT_COLON,b':')=>self.stack.set(OBJECT_VALUE)?,
            (OBJECT_AFTER,b',')=>self.stack.set(OBJECT_KEY)?,
            (ARRAY_AFTER,b',')=>self.stack.set(ARRAY_VALUE)?,
            (OBJECT_VALUE | ARRAY_FIRST | ARRAY_VALUE,_)=>self.value(byte,budget)?,
            _=>self.result=Some(false),
        }
        Ok(())
    }
    fn value(&mut self, byte: u8, budget: &mut Budget) -> Result<()> {
        let root_format = self.stack.len()==1 && self.format_key;
        let after = if self.stack.top()?==OBJECT_VALUE {OBJECT_AFTER} else {ARRAY_AFTER};
        self.stack.set(after)?;
        self.token = match byte {
            b'"'=>Token::String(Text::new(Role::Value,if root_format {PROTOCOL} else {b""})),
            b'{'=>{self.stack.push(OBJECT_FIRST,budget)?;Token::None},
            b'['=>{self.stack.push(ARRAY_FIRST,budget)?;Token::None},
            b't'=>Token::Literal {expected:b"true",next:1},
            b'f'=>Token::Literal {expected:b"false",next:1},
            b'n'=>Token::Literal {expected:b"null",next:1},
            b'-'=>Token::Number(Number::Sign),
            b'0'=>Token::Number(Number::Zero),
            b'1'..=b'9'=>Token::Number(Number::Integer),
            _=>{self.result=Some(false);Token::None},
        };
        Ok(())
    }
}
