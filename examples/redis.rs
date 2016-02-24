extern crate td_rredis as redis;
use std::error;
use std::fmt;
use std::io;
use std::hash::Hash;
use std::str::{from_utf8, Utf8Error};
use std::collections::{HashMap, HashSet};
use std::convert::From;

use redis::RedisResult;


fn test_get() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();

    redis::cmd("DEL").arg("foo").execute(&con);
    for x in 0..600 {
        redis::cmd("SADD").arg("foo").arg(x).execute(&con);
    }
    Ok(())
}

fn main() {
    let _ = test_get();
}