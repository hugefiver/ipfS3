//! Lossless private record packing, never an identity/fence or wire capability.
//! Native frames retain the exact stored bytes of every foreign record. Decoding
//! restores all original evidence and is bounded by the existing JSON budget.
use super::*;
use base64::{Engine,engine::general_purpose::STANDARD};
use std::collections::{BTreeMap,VecDeque};
const PREFIX:&str="stage7/lz1:";
const WINDOW:usize=u16::MAX as usize;
const CANDIDATES:usize=32;
const STRING_KEY:&str="$stage7_private_string";

pub(super) fn pack(input:&str)->Result<String> {
    crate::pinning::management::wire::check_structure(input.as_bytes()).map_err(|_|ManagementError::Budget)?;
    let mut value:StrictValue=serde_json::from_str(input).map_err(|_|ManagementError::InvalidEvidence)?;
    pack_strings(&mut value.0)?;
    let packed=serde_json::to_string(&value.0).map_err(|_|ManagementError::InvalidEvidence)?;
    crate::pinning::management::wire::check_structure(packed.as_bytes()).map_err(|_|ManagementError::Budget)?;Ok(packed)
}
fn pack_strings(value:&mut serde_json::Value)->Result<()> {
    match value {
        serde_json::Value::String(text) if text.len()>=4096=>{
            let packed=pack_payload(text);
            if packed.len()+STRING_KEY.len()+8<text.len(){let mut fields=serde_json::Map::new();fields.insert(STRING_KEY.into(),serde_json::Value::String(packed));*value=serde_json::Value::Object(fields);}
        },
        serde_json::Value::Array(values)=>for value in values{pack_strings(value)?;},
        serde_json::Value::Object(values)=>for value in values.values_mut(){pack_strings(value)?;},
        _=>{},
    }Ok(())
}
fn pack_payload(input:&str)->String {
    let bytes=input.as_bytes();let mut index=0;let mut literal=Vec::new();let mut encoded=Vec::new();
    let mut dictionary=BTreeMap::<[u8;3],VecDeque<usize>>::new();
    while index<bytes.len() {
        let mut best=(0,0);
        if index+3<=bytes.len() {
            let key=[bytes[index],bytes[index+1],bytes[index+2]];
            if let Some(positions)=dictionary.get_mut(&key) {
                while positions.front().is_some_and(|position|index-position>WINDOW) {positions.pop_front();}
                for &position in positions.iter().rev() {
                    let mut length=3;
                    while length<WINDOW&&index+length<bytes.len()&&bytes[position+length]==bytes[index+length] {length+=1;}
                    if length>best.1 {best=(index-position,length);}
                }
            }
        }
        let advance=if best.1>=8 {
            flush(&mut literal,&mut encoded);
            encoded.push(1);encoded.extend_from_slice(&(best.0 as u16).to_le_bytes());encoded.extend_from_slice(&(best.1 as u16).to_le_bytes());best.1
        }else{literal.push(bytes[index]);if literal.len()==WINDOW {flush(&mut literal,&mut encoded);}1};
        for position in index..index+advance {
            if position+3>bytes.len() {break;}
            if position>=WINDOW {
                let expired=position-WINDOW;let key=[bytes[expired],bytes[expired+1],bytes[expired+2]];
                if let Some(entry)=dictionary.get_mut(&key) {while entry.front().is_some_and(|&old|old<=expired){entry.pop_front();}if entry.is_empty(){dictionary.remove(&key);}}
            }
            let entry=dictionary.entry([bytes[position],bytes[position+1],bytes[position+2]]).or_default();
            if entry.len()==CANDIDATES {entry.pop_front();}entry.push_back(position);
        }
        index+=advance;
    }
    flush(&mut literal,&mut encoded);
    let packed=format!("{PREFIX}{}:{}",bytes.len(),STANDARD.encode(encoded));
    // Keep incompressible historical encodings byte-for-byte, not a larger
    // representation that would consume the native DB inventory budget.
    if packed.len()<input.len(){packed}else{input.to_owned()}
}

fn flush(literal:&mut Vec<u8>,output:&mut Vec<u8>) {
    if literal.is_empty(){return;}output.push(0);output.extend_from_slice(&(literal.len() as u16).to_le_bytes());output.append(literal);
}

pub(super) fn unpack(input:&str)->Result<String> {
    let unpacked=if input.starts_with(PREFIX){unpack_payload(input)?}else{input.to_owned()};
    crate::pinning::management::wire::check_structure(unpacked.as_bytes()).map_err(|_|ManagementError::Budget)?;
    let mut value:StrictValue=serde_json::from_str(&unpacked).map_err(|_|ManagementError::InvalidEvidence)?;
    let mut bytes=0usize;unpack_strings(&mut value.0,&mut bytes)?;
    let output=serde_json::to_string(&value.0).map_err(|_|ManagementError::InvalidEvidence)?;
    crate::pinning::management::wire::check_structure(output.as_bytes()).map_err(|_|ManagementError::Budget)?;Ok(output)
}
fn unpack_strings(value:&mut serde_json::Value,bytes:&mut usize)->Result<()> {
    if let serde_json::Value::Object(values)=value&&values.len()==1&&let Some(serde_json::Value::String(packed))=values.get(STRING_KEY) {
        if !packed.starts_with(PREFIX){return Err(ManagementError::InvalidEvidence);}
        let declared=packed[PREFIX.len()..].split_once(':').and_then(|(count,_)|count.parse::<usize>().ok()).ok_or(ManagementError::InvalidEvidence)?;
        if bytes.checked_add(declared).is_none_or(|sum|sum>crate::pinning::management::wire::MAX_WIRE_BYTES){return Err(ManagementError::Budget);}
        *value=serde_json::Value::String(unpack_payload(packed)?);
    }
    match value {
        serde_json::Value::String(text)=>{*bytes=bytes.checked_add(text.len()).ok_or(ManagementError::Budget)?;if *bytes>crate::pinning::management::wire::MAX_WIRE_BYTES{return Err(ManagementError::Budget);}},
        serde_json::Value::Array(values)=>for value in values{unpack_strings(value,bytes)?;},
        serde_json::Value::Object(values)=>for value in values.values_mut(){unpack_strings(value,bytes)?;},
        _=>{},
    }Ok(())
}
fn unpack_payload(input:&str)->Result<String> {
    if !input.starts_with(PREFIX) {return Ok(input.to_owned());}
    if input.len()>crate::pinning::management::wire::MAX_WIRE_BYTES {return Err(ManagementError::Budget);}
    let (length,data)=input[PREFIX.len()..].split_once(':').ok_or(ManagementError::InvalidEvidence)?;
    if length.is_empty()||length.len()>10||!length.bytes().all(|byte|byte.is_ascii_digit()) {return Err(ManagementError::InvalidEvidence);}
    let length=length.parse::<usize>().map_err(|_|ManagementError::InvalidEvidence)?;
    if length>crate::pinning::management::wire::MAX_WIRE_BYTES {return Err(ManagementError::Budget);}
    let encoded=STANDARD.decode(data).map_err(|_|ManagementError::InvalidEvidence)?;
    let mut output=Vec::with_capacity(length);let mut index=0;
    while index<encoded.len() {
        let kind=encoded[index];index+=1;
        let first=word(&encoded,&mut index)? as usize;
        match kind {
            0=>{
                if first==0||output.len().checked_add(first).is_none_or(|n|n>length)||index.checked_add(first).is_none_or(|n|n>encoded.len()) {return Err(ManagementError::InvalidEvidence);}
                output.extend_from_slice(&encoded[index..index+first]);index+=first;
            },
            1=>{
                let count=word(&encoded,&mut index)? as usize;
                if first==0||first>output.len()||count<8||output.len().checked_add(count).is_none_or(|n|n>length) {return Err(ManagementError::InvalidEvidence);}
                for _ in 0..count {output.push(output[output.len()-first]);}
            },
            _=>return Err(ManagementError::InvalidEvidence),
        }
    }
    if output.len()!=length {return Err(ManagementError::InvalidEvidence);}
    let output=String::from_utf8(output).map_err(|_|ManagementError::InvalidEvidence)?;
    Ok(output)
}
fn word(input:&[u8],index:&mut usize)->Result<u16> {
    let end=index.checked_add(2).ok_or(ManagementError::InvalidEvidence)?;
    let bytes=input.get(*index..end).ok_or(ManagementError::InvalidEvidence)?;*index=end;Ok(u16::from_le_bytes([bytes[0],bytes[1]]))
}

struct StrictValue(serde_json::Value);
impl<'de> serde::Deserialize<'de> for StrictValue {
    fn deserialize<D:serde::Deserializer<'de>>(deserializer:D)->std::result::Result<Self,D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value=StrictValue;
            fn expecting(&self,formatter:&mut std::fmt::Formatter)->std::fmt::Result {formatter.write_str("bounded JSON without duplicate fields")}
            fn visit_bool<E:serde::de::Error>(self,value:bool)->std::result::Result<StrictValue,E>{Ok(StrictValue(value.into()))}
            fn visit_i64<E:serde::de::Error>(self,value:i64)->std::result::Result<StrictValue,E>{Ok(StrictValue(value.into()))}
            fn visit_u64<E:serde::de::Error>(self,value:u64)->std::result::Result<StrictValue,E>{Ok(StrictValue(value.into()))}
            fn visit_f64<E:serde::de::Error>(self,value:f64)->std::result::Result<StrictValue,E>{serde_json::Number::from_f64(value).map(|value|StrictValue(value.into())).ok_or_else(||E::custom("nonfinite JSON number"))}
            fn visit_str<E:serde::de::Error>(self,value:&str)->std::result::Result<StrictValue,E>{Ok(StrictValue(value.into()))}
            fn visit_string<E:serde::de::Error>(self,value:String)->std::result::Result<StrictValue,E>{Ok(StrictValue(value.into()))}
            fn visit_unit<E:serde::de::Error>(self)->std::result::Result<StrictValue,E>{Ok(StrictValue(serde_json::Value::Null))}
            fn visit_seq<A:serde::de::SeqAccess<'de>>(self,mut access:A)->std::result::Result<StrictValue,A::Error>{let mut values=Vec::new();while let Some(value)=access.next_element::<StrictValue>()?{values.push(value.0);}Ok(StrictValue(values.into()))}
            fn visit_map<A:serde::de::MapAccess<'de>>(self,mut access:A)->std::result::Result<StrictValue,A::Error>{let mut values=serde_json::Map::new();while let Some((key,value))=access.next_entry::<String,StrictValue>()?{if values.insert(key,value.0).is_some(){return Err(serde::de::Error::custom("duplicate JSON field"));}}Ok(StrictValue(values.into()))}
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packing_preserves_nested_private_bytes_and_rejects_invalid_backreferences() {
        let body=serde_json::to_string(&vec!["private\\\"original tuple".repeat(400)]).unwrap();
        let packed=pack(&body).unwrap();assert!(packed.len()<body.len());assert_eq!(unpack(&packed).unwrap(),body);
        assert!(unpack("stage7/lz1:8:AQAAAQA=").is_err());
        assert!(unpack("stage7/lz1:16777217:AA==").is_err());
        assert_eq!(unpack("{\"legacy\":true}").unwrap(),"{\"legacy\":true}");
        assert!(unpack("{\"legacy\":true,\"legacy\":false}").is_err());
        let marker=serde_json::json!({"private_marker":"RETIREMENT_METADATA_MARKER","nested":body});
        let packed=pack(&marker.to_string()).unwrap();assert!(packed.contains("RETIREMENT_METADATA_MARKER"));assert_eq!(serde_json::from_str::<serde_json::Value>(&unpack(&packed).unwrap()).unwrap(),marker);
    }
}
