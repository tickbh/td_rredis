extern crate td_rredis as redis;

use redis::{Commands, PipelineCommands};

use std::thread::{spawn, sleep};
use std::time::Duration;
use std::collections::{HashMap, HashSet};

pub static SERVER_PORT: u16 = 6379;


pub struct TestContext {
    pub client: redis::Client,
}

impl TestContext {

    fn new() -> TestContext {
        let client = redis::Client::open(&format!("redis://127.0.0.1:{}/10", SERVER_PORT)[..]).unwrap();
        TestContext {
            client: client,
        }
    }

    fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    fn pubsub(&self) -> redis::PubSub {
        self.client.get_pubsub().unwrap()
    }


}

fn del_key(con : &redis::Connection, keys : Vec<&str>) {
    for key in keys {
        redis::cmd("DEL").arg(key).execute(con);
    }
}


#[test]
fn test_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let (key1, key2) = ("test_args_key1", "test_args_key2");
    del_key(&con, vec![key1, key2]);
    redis::cmd("SET").arg(key1).arg(b"foo").execute(&con);
    redis::cmd("SET").arg(&[key2, "bar"]).execute(&con);

    assert_eq!(redis::cmd("MGET").arg(&[key1, key2]).query(&con),
               Ok(("foo".to_string(), b"bar".to_vec())));
    del_key(&con, vec![key1, key2]);
}

#[test]
fn test_getset() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_getset";
    del_key(&con, vec![key1]);
    redis::cmd("SET").arg(key1).arg(42).execute(&con);
    assert_eq!(redis::cmd("GET").arg(key1).query(&con), Ok(42));

    redis::cmd("SET").arg(key1).arg("foo").execute(&con);
    assert_eq!(redis::cmd("GET").arg(key1).query(&con), Ok(b"foo".to_vec()));
    del_key(&con, vec![key1]);
}

#[test]
fn test_incr() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_incr";
    del_key(&con, vec![key1]);
    redis::cmd("SET").arg(key1).arg(42).execute(&con);
    assert_eq!(redis::cmd("INCR").arg(key1).query(&con), Ok(43usize));
    del_key(&con, vec![key1]);

}

#[test]
fn test_info() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let info : redis::InfoDict = redis::cmd("INFO").query(&con).unwrap();
    assert_eq!(info.find(&"role"), Some(&redis::Value::Status("master".to_string())));
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(info.len() > 0);
    assert!(info.contains_key(&"role"));
}

#[test]
fn test_hash_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_hash_ops";
    del_key(&con, vec![key1]);
    redis::cmd("HSET").arg(key1).arg("key_1").arg(1).execute(&con);
    redis::cmd("HSET").arg(key1).arg("key_2").arg(2).execute(&con);

    let h : HashMap<String, i32> = redis::cmd("HGETALL").arg(key1).query(&con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
    del_key(&con, vec![key1]);
}

#[test]
fn test_set_ops() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_set_ops";
    del_key(&con, vec![key1]);
    redis::cmd("SADD").arg(key1).arg(1).execute(&con);
    redis::cmd("SADD").arg(key1).arg(2).execute(&con);
    redis::cmd("SADD").arg(key1).arg(3).execute(&con);

    let mut s : Vec<i32> = redis::cmd("SMEMBERS").arg(key1).query(&con).unwrap();
    s.sort();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set : HashSet<i32> = redis::cmd("SMEMBERS").arg(key1).query(&con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
    del_key(&con, vec![key1]);
}


#[test]
fn test_scan() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_scan";
    del_key(&con, vec![key1]);
    redis::cmd("SADD").arg(key1).arg(1).execute(&con);
    redis::cmd("SADD").arg(key1).arg(2).execute(&con);
    redis::cmd("SADD").arg(key1).arg(3).execute(&con);

    let (cur, mut s) : (i32, Vec<i32>) = redis::cmd("SSCAN").arg(key1).arg(0).query(&con).unwrap();
    s.sort();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
    del_key(&con, vec![key1]);
}

#[test]
fn test_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();
    let key1 = "test_scanning";
    del_key(&con, vec![key1]);
    for x in 0..1000 {
        redis::cmd("SADD").arg(key1).arg(x).execute(&con);
        unseen.insert(x);
    }

    let iter = redis::cmd("SSCAN").arg(key1).cursor_arg(0).iter(&con).unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
    del_key(&con, vec![key1]);
}


#[test]
fn test_filtered_scanning() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let mut unseen = HashSet::new();
    let key1 = "test_filtered_scanning";
    del_key(&con, vec![key1]);
    for x in 0..3000 {
        let _ : () = con.hset(key1, format!("key_{}_{}", x % 100, x), x).unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let iter = con.hscan_match(key1, "key_0_*").unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
    del_key(&con, vec![key1]);
}

#[test]
fn test_optionals() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_optionals";
    del_key(&con, vec![key1]);
 
    redis::cmd("SET").arg(key1).arg(1).execute(&con);

    let (a, b) : (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg(key1).arg("missing").query(&con).unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET").arg("missing").query(&con).unwrap_or(0i32);
    assert_eq!(a, 0i32);
    del_key(&con, vec![key1]);
}

#[test]
fn test_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let (key1, key2) = ("test_pipeline_key_1", "test_pipeline_key_2");
    del_key(&con, vec![key1, key2]);

    let ((k1, k2),) : ((i32, i32),) = redis::pipe()
        .cmd("SET").arg(key1).arg(42).ignore()
        .cmd("SET").arg(key2).arg(43).ignore()
        .cmd("MGET").arg(&[key1, key2]).query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
    del_key(&con, vec![key1, key2]);
}

#[test]
fn test_empty_pipeline() {
    let ctx = TestContext::new();
    let con = ctx.connection();

    let _ : () = redis::pipe()
        .cmd("PING").ignore()
        .query(&con).unwrap();

    let _ : () = redis::pipe().query(&con).unwrap();
}

#[test]
fn test_pipeline_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let (key1, key2) = ("test_pipeline_transaction_key_1", "test_pipeline_transaction_key_2");
    del_key(&con, vec![key1, key2]);

    let ((k1, k2),) : ((i32, i32),) = redis::pipe()
        .atomic()
        .cmd("SET").arg(key1).arg(42).ignore()
        .cmd("SET").arg(key2).arg(43).ignore()
        .cmd("MGET").arg(&[key1, key2]).query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
    del_key(&con, vec![key1, key2]);
}

#[test]
fn test_real_transaction() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_real_transaction_key";
    del_key(&con, vec![key1]);

    let _ : () = redis::cmd("SET").arg(key1).arg(42).query(&con).unwrap();

    loop {
        let _ : () = redis::cmd("WATCH").arg(key1).query(&con).unwrap();
        let val : isize = redis::cmd("GET").arg(key1).query(&con).unwrap();
        let response : Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET").arg(key1).arg(val + 1).ignore()
            .cmd("GET").arg(key1)
            .query(&con).unwrap();

        match response {
            None => { continue; }
            Some(response) => {
                assert_eq!(response, (43,));
                break;
            }
        }
    }
    del_key(&con, vec![key1]);
}

#[test]
fn test_real_transaction_highlevel() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_real_transaction_highlevel";
    del_key(&con, vec![key1]);

    let _ : () = redis::cmd("SET").arg(key1).arg(42).query(&con).unwrap();

    let response : (isize,) = redis::transaction(&con, &[key1], |pipe| {
        let val : isize = try!(redis::cmd("GET").arg(key1).query(&con));
        pipe
            .cmd("SET").arg(key1).arg(val + 1).ignore()
            .cmd("GET").arg(key1).query(&con)
    }).unwrap();

    assert_eq!(response, (43,));
    del_key(&con, vec![key1]);
}

#[test]
fn test_pubsub() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let channel = "test_pubsub";
    let mut pubsub = ctx.pubsub();
    pubsub.subscribe(channel).unwrap();

    let thread = spawn(move || {
        sleep(Duration::from_millis(100));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok(channel.to_string()));
        assert_eq!(msg.get_payload(), Ok(42));

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel(), Ok(channel.to_string()));
        assert_eq!(msg.get_payload(), Ok(23));
    });

    redis::cmd("PUBLISH").arg(channel).arg(42).execute(&con);
    redis::cmd("PUBLISH").arg(channel).arg(23).execute(&con);

    thread.join().ok().expect("Something went wrong");
}

#[test]
fn test_script() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_script";
    del_key(&con, vec![key1]);

    let script = redis::Script::new(r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ");

    let _ : () = redis::cmd("SET").arg(key1).arg("foo").query(&con).unwrap();
    let response = script.key(key1).arg(42).invoke(&con);

    assert_eq!(response, Ok(("foo".to_string(), 42)));
    del_key(&con, vec![key1]);
}

#[test]
fn test_tuple_args() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_tuple_args";
    del_key(&con, vec![key1]);
    redis::cmd("HMSET").arg(key1).arg(&[
        ("field_1", 42),
        ("field_2", 23),
    ]).execute(&con);

    assert_eq!(redis::cmd("HGET").arg(key1).arg("field_1").query(&con), Ok(42));
    assert_eq!(redis::cmd("HGET").arg(key1).arg("field_2").query(&con), Ok(23));
    del_key(&con, vec![key1]);
}

#[test]
fn test_nice_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_nice_api";
    del_key(&con, vec![key1]);
    assert_eq!(con.set(key1, 42), Ok(()));
    assert_eq!(con.get(key1), Ok(42));
    del_key(&con, vec![key1]);

    let (key1, key2) = ("test_nice_api_key_1", "test_nice_api_key_2");
    del_key(&con, vec![key1, key2]);
    let (k1, k2) : (i32, i32) = redis::pipe()
        .atomic()
        .set(key1, 42).ignore()
        .set(key2, 43).ignore()
        .get(key1)
        .get(key2).query(&con).unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
    del_key(&con, vec![key1, key2]);
}

#[test]
fn test_auto_m_versions() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let (key1, key2) = ("test_auto_m_versions_key1", "test_auto_m_versions_key2");
    del_key(&con, vec![key1, key2]);
    assert_eq!(con.set_multiple(&[(key1, 1), (key2, 2)]), Ok(()));
    assert_eq!(con.get(&[key1, key2]), Ok((1, 2)));
    del_key(&con, vec![key1, key2]);}

#[test]
fn test_nice_list_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_nice_list_api";
    del_key(&con, vec![key1]);
    assert_eq!(con.rpush(key1, &[1, 2, 3, 4]), Ok(4));
    assert_eq!(con.rpush(key1, &[5, 6, 7, 8]), Ok(8));
    assert_eq!(con.llen(key1), Ok(8));

    assert_eq!(con.lpop(key1), Ok(1));
    assert_eq!(con.llen(key1), Ok(7));

    assert_eq!(con.lrange(key1, 0, 2), Ok((2, 3, 4)));
    del_key(&con, vec![key1]);
}


#[test]
fn test_nice_hash_api() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "my_hash";
    del_key(&con, vec![key1]);
    assert_eq!(con.hset_multiple(key1, &[
        ("f1", 1),
        ("f2", 2),
        ("f3", 4),
        ("f4", 8),
    ]), Ok(()));

    let hm : HashMap<String, isize> = con.hgetall(key1).unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v : Vec<(String, isize)> = con.hgetall(key1).unwrap();
    assert_eq!(v, vec![
        ("f1".to_string(), 1),
        ("f2".to_string(), 2),
        ("f3".to_string(), 4),
        ("f4".to_string(), 8),
    ]);

    assert_eq!(con.hget(key1, &["f2", "f4"]), Ok((2, 8)));
    assert_eq!(con.hincr(key1, "f1", 1), Ok((2)));
    assert_eq!(con.hincr(key1, "f2", 1.5f32), Ok((3.5f32)));
    assert_eq!(con.hexists(key1, "f2"), Ok(true));
    assert_eq!(con.hdel(key1, &["f1", "f2"]), Ok(()));
    assert_eq!(con.hexists(key1, "f2"), Ok(false));

    let iter : redis::Iter<(String, isize)> = con.hscan(key1).unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert_eq!(found.contains(&("f3".to_string(), 4)), true);
    assert_eq!(found.contains(&("f4".to_string(), 8)), true);
    del_key(&con, vec![key1]);
}

#[test]
fn test_tuple_decoding_regression() {
    let ctx = TestContext::new();
    let con = ctx.connection();
    let key1 = "test_tuple_decoding_regression";
    del_key(&con, vec![key1]);
    assert_eq!(con.del(key1), Ok(()));
    assert_eq!(con.zadd(key1, "one", 1), Ok(1));
    assert_eq!(con.zadd(key1, "two", 2), Ok(1));

    let vec : Vec<(String,u32)> = con.zrangebyscore_withscores(key1, 0, 10).unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(con.del(key1), Ok(1));

    let vec : Vec<(String,u32)> = con.zrangebyscore_withscores(key1, 0, 10).unwrap();
    assert_eq!(vec.len(), 0);
    del_key(&con, vec![key1]);
}

#[test]
fn test_invalid_protocol() {
    use std::thread;
    use std::error::Error;
    use std::io::Write;
    use std::net::TcpListener;
    use redis::{RedisResult, Parser};
    static PORT : i16 = 12345;
    let child = thread::spawn(move || -> Result<(), Box<Error + Send + Sync>> {
        let listener = try!(TcpListener::bind(&format!("127.0.0.1:{}", PORT)[..]));
        let mut stream = try!(listener.incoming().next().unwrap());
        // read the request and respond with garbage
        let _: redis::Value = try!(Parser::new(&mut stream).parse_value());
        try!(stream.write_all(b"garbage ---!#!#\r\n\r\n\n\r"));
        // block until the stream is shutdown by the client
        let _: RedisResult<redis::Value> = Parser::new(&mut stream).parse_value();
        Ok(())
    });
    sleep(Duration::from_millis(100));
    // some work here
    let cli = redis::Client::open(&format!("redis://127.0.0.1:{}/", PORT)[..]).unwrap();
    let con = cli.get_connection().unwrap();

    let mut result: redis::RedisResult<u8>;
    // first requests returns ResponseError
    result = con.del("my_zset");
    assert_eq!(result.unwrap_err().kind(), redis::ErrorKind::PatternError);
    // from now on it's IoError due to the closed connection
    result = con.del("my_zset");
    assert_eq!(result.unwrap_err().kind(), redis::ErrorKind::IoError);

    child.join().unwrap().unwrap();
}

#[test]
fn test_cluster() {
    let mut cluster = redis::Cluster::new();
    for i in 7000 .. 7001 {
        cluster.add(&format!("redis://192.168.1.135:{}/", i)[..]).unwrap();    
    }

    let _ : () = redis::cmd("set").arg("xxoo1").arg("ooxx").query_cluster(&mut cluster).unwrap();
    assert_eq!(redis::cmd("get").arg("xxoo1").query_cluster(&mut cluster), Ok("ooxx".to_string()));
}