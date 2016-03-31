extern crate td_rredis as redis;

use std::collections::HashMap;
use redis::{RedisResult, FromRedisValue};

fn test_cluster() -> redis::RedisResult<()> {
    let mut cluster = redis::Cluster::new();
    for i in 7000 .. 7001 {
        try!(cluster.add(&format!("redis://192.168.1.135:{}/", i)[..]));    
    }
    let mut cmd = redis::cmd("set");
    cmd.arg("foo11232").arg(02);
    try!(cluster.query_cmd(&cmd));
    let mut cmd = redis::cmd("get");
    cmd.arg("foo11232");
    let value : i32 = try!(cluster.query_cmd(&cmd));
    println!("value = {:?}", value);
    try!(redis::cmd("set").arg("xxoo1").arg("ooxx").query_cluster(&mut cluster));
    assert_eq!(redis::cmd("get").arg("xxoo1").query_cluster(&mut cluster), Ok("ooxx".to_string()));

    println!("ok {:?}", 11);

    try!(redis::cmd("SET").arg("key1").arg(b"foo").query_cluster(&mut cluster));
    println!("1111111111");
    try!(redis::cmd("SET").arg(&["key5", "bar"]).query_cluster(&mut cluster));
    println!("222222222");
    let xx : redis::RedisResult<(String, String)> = redis::cmd("MGET").arg(&["key1", "key1"]).query_cluster(&mut cluster);
    println!("33333333333 = {:?}", xx);

    let (xx, xx1) : (String, String) = try!(redis::cmd("MGET").arg(&["key1", "key5"]).query_cluster(&mut cluster));
    println!("xx = {:?}", xx);
    Ok(())
}

fn main() {
    let mut test_insert : HashMap<String, String> = HashMap::new();
    test_insert.insert("kk".to_string(), "xx".to_string());
    println!("{:?}", test_insert.get(&"kk".to_string()));
    test_insert.insert("kk".to_string(), "oo".to_string());
    println!("{:?}", test_insert.get(&"kk".to_string()));
    let _ = test_cluster();

}