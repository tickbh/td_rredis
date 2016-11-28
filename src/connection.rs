use std::io::{Read, BufReader, Write};
use std::net::{self, TcpStream};
use std::str::from_utf8;
use std::cell::RefCell;
use std::collections::HashSet;

use url;

use types::{RedisResult, Value, ToRedisArgs, FromRedisValue, from_redis_value, ErrorKind};
use parser::Parser;
use cmd::{cmd, pipe, Pipeline};

static DEFAULT_PORT: u16 = 6379;

/// Holds the connection information that redis should use for connecting.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A boxed connection address for where to connect to.
    pub host: String,

    pub port: u16,
    /// The database number to use.  This is usually `0`.
    pub db: i64,
    /// Optionally a password that should be used for connection.
    pub passwd: Option<String>,
}

/// Represents a stateful redis TCP connection.
#[derive(Debug)]
pub struct Connection {
    con: RefCell<BufReader<TcpStream>>,
    db: i64,
    work: RefCell<bool>,
}

/// Represents a pubsub connection.
#[derive(Debug)]
pub struct PubSub {
    con: Connection,
    channels: HashSet<String>,
    pchannels: HashSet<String>,
}

/// Represents a pubsub message.
pub struct Msg {
    payload: Value,
    channel: Value,
    pattern: Option<Value>,
}

fn redis_scheme_type_mapper(_scheme: &str) -> url::SchemeType {
    url::SchemeType::Relative(DEFAULT_PORT)
}

/// This function takes a redis URL string and parses it into a URL
/// as used by rust-url.  This is necessary as the default parser does
/// not understand how redis URLs function.
fn parse_redis_url(input: &str) -> url::ParseResult<url::Url> {
    let mut parser = url::UrlParser::new();
    parser.scheme_type_mapper(redis_scheme_type_mapper);
    match parser.parse(input) {
        Ok(result) => {
            if result.scheme == "redis" {
                Ok(result)
            } else {
                Err(url::ParseError::InvalidScheme)
            }
        }
        Err(err) => Err(err),
    }
}

fn url_to_connection_info(url: url::Url) -> RedisResult<ConnectionInfo> {
    Ok(ConnectionInfo {
        host: unwrap_or!(url.serialize_host(),
                         fail!((ErrorKind::InvalidClientConfig, "Missing hostname"))),
        port: url.port().unwrap_or(DEFAULT_PORT),
        db: match url.serialize_path().unwrap_or("".to_string()).trim_matches('/') {
            "" => 0,
            path => {
                unwrap_or!(path.parse::<i64>().ok(),
                           fail!((ErrorKind::InvalidClientConfig, "Invalid database number")))
            }
        },
        passwd: url.password().and_then(|pw| Some(pw.to_string())),
    })
}

pub fn into_connection_info(info: &str) -> RedisResult<ConnectionInfo> {
    match parse_redis_url(info) {
        Ok(u) => url_to_connection_info(u),
        Err(_) => fail!((ErrorKind::InvalidClientConfig, "Redis URL did not parse")),
    }
}

pub fn connect(connection_info: &ConnectionInfo) -> RedisResult<Connection> {
    let con = try!(Connection::new(&connection_info));
    let mut rv = Connection {
        con: RefCell::new(con),
        db: connection_info.db,
        work: RefCell::new(true),
    };

    match connection_info.passwd {
        Some(ref passwd) => {
            match cmd("AUTH").arg(&**passwd).query::<Value>(&mut rv) {
                Ok(Value::Okay) => {}
                Err(err) => {
                    if err.kind() != ErrorKind::ResponseError ||
                       err.extension_error_detail()
                          .unwrap_or("")
                          .find("but no password is set")
                          .is_none() {
                        fail!((ErrorKind::AuthenticationFailed, "Password authentication failed"));
                    }
                }
                _ => {
                    fail!((ErrorKind::AuthenticationFailed, "Password authentication failed"));
                }
            }
        }
        None => {}
    }

    if connection_info.db != 0 {
        match cmd("SELECT").arg(connection_info.db).query::<Value>(&mut rv) {
            Ok(Value::Okay) => {}
            _ => fail!((ErrorKind::ResponseError, "Redis server refused to switch database")),
        }
    }

    Ok(rv)
}

pub fn connect_pubsub(connection_info: &ConnectionInfo) -> RedisResult<PubSub> {
    Ok(PubSub {
        con: try!(connect(connection_info)),
        channels: HashSet::new(),
        pchannels: HashSet::new(),
    })
}

impl Connection {
    pub fn new(addr: &ConnectionInfo) -> RedisResult<BufReader<TcpStream>> {
        let tcp = try!(TcpStream::connect((&*addr.host, addr.port)));
        Ok(BufReader::new(tcp))
    }

    pub fn send_bytes(&self, bytes: &[u8]) -> RedisResult<Value> {
        let mut buffer = self.con.borrow_mut();
        let w = buffer.get_mut() as &mut Write;
        try!(w.write(bytes));
        Ok(Value::Okay)
    }

    pub fn read_response(&self) -> RedisResult<Value> {
        let mut buffer = self.con.borrow_mut();
        let result = Parser::new(buffer.get_mut() as &mut Read).parse_value();
        // shutdown connection on protocol error
        match result {
            Err(ref e) if e.kind() == ErrorKind::PatternError => {
                try!(buffer.get_mut().shutdown(net::Shutdown::Both));
                *self.work.borrow_mut() = false;
            }
            _ => (),
        }
        result
    }

    pub fn get_connection_fd(&self) -> i32 {
        #[cfg(unix)]
        use std::os::unix::prelude::*;
        #[cfg(windows)]
        use std::os::windows::prelude::*;
        #[cfg(windows)]
        fn get_fd(tcp: &TcpStream) -> i32 {
            tcp.as_raw_socket() as i32
        }
        #[cfg(unix)]
        fn get_fd(tcp: &TcpStream) -> i32 {
            tcp.as_raw_fd() as i32
        }
        let buffer = self.con.borrow();
        let tcp = buffer.get_ref();
        get_fd(tcp)
    }

    pub fn is_work(&self) -> bool {
        *self.work.borrow()
    }

    pub fn try_clone(&self) -> RedisResult<Connection> {
        let tcp = try!(self.con.borrow().get_ref().try_clone());
        Ok(Connection {
            con: RefCell::new(BufReader::new(tcp)),
            db: self.db,
            work: RefCell::new(self.is_work()),
        })
    }
}

/// Implements the "stateless" part of the connection interface that is used by the
/// different objects in redis-rs.  Primarily it obviously applies to `Connection`
/// object but also some other objects implement the interface (for instance
/// whole clients or certain redis results).
///
/// Generally clients and connections (as well as redis results of those) implement
/// this trait.  Actual connections provide more functionality which can be used
/// to implement things like `PubSub` but they also can modify the intrinsic
/// state of the TCP connection.  This is not possible with `ConnectionLike`
/// implementors because that functionality is not exposed.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>>;

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;
}



impl ConnectionLike for Connection {
    fn req_packed_command(&self, cmd: &[u8]) -> RedisResult<Value> {
        try!(self.send_bytes(cmd));
        self.read_response()
    }

    fn req_packed_commands(&self,
                           cmd: &[u8],
                           offset: usize,
                           count: usize)
                           -> RedisResult<Vec<Value>> {
        try!(self.send_bytes(cmd));
        let mut rv = vec![];
        for idx in 0..(offset + count) {
            let item = try!(self.read_response());
            if idx >= offset {
                rv.push(item);
            }
        }
        Ok(rv)
    }

    fn get_db(&self) -> i64 {
        self.db
    }
}


/// The pubsub object provides convenient access to the redis pubsub
/// system.  Once created you can subscribe and unsubscribe from channels
/// and listen in on messages.
///
/// Example:
///
/// ```rust,no_run
/// # fn do_something() -> td_rredis::RedisResult<()> {
/// let client = try!(td_rredis::Client::open("redis://127.0.0.1/"));
/// let mut pubsub = try!(client.get_pubsub());
/// try!(pubsub.subscribe("channel_1"));
/// try!(pubsub.subscribe("channel_2"));
///
/// loop {
///     let msg = try!(pubsub.get_message());
///     let payload : String = try!(msg.get_payload());
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// # }
/// ```
impl PubSub {
    /// Subscribes to a new channel.
    pub fn subscribe(&mut self, channel: String) -> RedisResult<()> {
        self.subscribes(vec![channel])
    }

    pub fn subscribes(&mut self, channels: Vec<String>) -> RedisResult<()> {
        let mut cmd = cmd("SUBSCRIBE");
        for channel in &channels {
            cmd.arg(&**channel);
        }
        let _: () = try!(cmd.query(&self.con));
        self.channels.extend(channels);
        Ok(())
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribe(&mut self, channel: String) -> RedisResult<()> {
        self.psubscribes(vec![channel])
    }

    /// Subscribes to a new channel with a pattern.
    pub fn psubscribes(&mut self, channels: Vec<String>) -> RedisResult<()> {
        let mut cmd = cmd("PSUBSCRIBE");
        for channel in &channels {
            cmd.arg(&**channel);
        }
        let _: () = try!(cmd.query(&self.con));
        self.pchannels.extend(channels);
        Ok(())
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribe(&mut self, channel: String) -> RedisResult<()> {
        self.unsubscribes(vec![channel])
    }

    /// Unsubscribes from a channel.
    pub fn unsubscribes(&mut self, channels: Vec<String>) -> RedisResult<()> {

        let mut cmd = cmd("UNSUBSCRIBE");
        for channel in &channels {
            cmd.arg(&**channel);
        }
        let _: () = try!(cmd.query(&self.con));
        for channel in channels {
            self.channels.remove(&channel);
        }
        Ok(())
    }

    /// Unsubscribes from a channel.
    pub fn punsubscribe(&mut self, channel: String) -> RedisResult<()> {
        self.punsubscribes(vec![channel])
    }

    /// Unsubscribes from a channel.
    pub fn punsubscribes(&mut self, channels: Vec<String>) -> RedisResult<()> {

        let mut cmd = cmd("PUNSUBSCRIBE");
        for channel in &channels {
            cmd.arg(&**channel);
        }
        let _: () = try!(cmd.query(&self.con));
        for channel in channels {
            self.pchannels.remove(&channel);
        }
        Ok(())
    }

    pub fn try_clone(&self) -> RedisResult<PubSub> {
        Ok(PubSub {
            con: try!(self.con.try_clone()),
            channels: self.channels.clone(),
            pchannels: self.pchannels.clone(),
        })
    } 

    pub fn is_work(&self) -> bool {
        self.con.is_work()
    }

    pub fn get_connection_fd(&self) -> i32 {
        self.con.get_connection_fd()
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    pub fn get_message(&self) -> RedisResult<Msg> {
        loop {
            let raw_msg: Vec<Value> = try!(from_redis_value(&try!(self.con.read_response())));
            let mut iter = raw_msg.into_iter();
            let msg_type: String = try!(from_redis_value(&unwrap_or!(iter.next(), continue)));
            let mut pattern = None;
            let payload;
            let channel;

            if msg_type == "message" {
                channel = unwrap_or!(iter.next(), continue);
                payload = unwrap_or!(iter.next(), continue);
            } else if msg_type == "pmessage" {
                pattern = Some(unwrap_or!(iter.next(), continue));
                channel = unwrap_or!(iter.next(), continue);
                payload = unwrap_or!(iter.next(), continue);
            } else {
                continue;
            }

            return Ok(Msg {
                payload: payload,
                channel: channel,
                pattern: pattern,
            });
        }
    }
}


/// This holds the data that comes from listening to a pubsub
/// connection.  It only contains actual message data.
impl Msg {
    /// Returns the channel this message came on.
    pub fn get_channel<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.channel)
    }

    /// Convenience method to get a string version of the channel.  Unless
    /// your channel contains non utf-8 bytes you can always use this
    /// method.  If the channel is not a valid string (which really should
    /// not happen) then the return value is `"?"`.
    pub fn get_channel_name(&self) -> &str {
        match self.channel {
            Value::Data(ref bytes) => from_utf8(bytes).unwrap_or("?"),
            _ => "?",
        }
    }

    /// Returns the message's payload in a specific format.
    pub fn get_payload<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.payload)
    }

    /// Returns the bytes that are the message's payload.  This can be used
    /// as an alternative to the `get_payload` function if you are interested
    /// in the raw bytes in it.
    pub fn get_payload_bytes(&self) -> &[u8] {
        match self.channel {
            Value::Data(ref bytes) => bytes,
            _ => b"",
        }
    }

    /// Returns true if the message was constructed from a pattern
    /// subscription.
    pub fn from_pattern(&self) -> bool {
        self.pattern.is_some()
    }

    /// If the message was constructed from a message pattern this can be
    /// used to find out which one.  It's recommended to match against
    /// an `Option<String>` so that you do not need to use `from_pattern`
    /// to figure out if a pattern was set.
    pub fn get_pattern<T: FromRedisValue>(&self) -> RedisResult<T> {
        match self.pattern {
            None => from_redis_value(&Value::Nil),
            Some(ref x) => from_redis_value(x),
        }
    }
}

/// This function simplifies transaction management slightly.  What it
/// does is automatically watching keys and then going into a transaction
/// loop util it succeeds.  Once it goes through the results are
/// returned.
///
/// To use the transaction two pieces of information are needed: a list
/// of all the keys that need to be watched for modifications and a
/// closure with the code that should be execute in the context of the
/// transaction.  The closure is invoked with a fresh pipeline in atomic
/// mode.  To use the transaction the function needs to return the result
/// from querying the pipeline with the connection.
///
/// The end result of the transaction is then available as the return
/// value from the function call.
///
/// Example:
///
/// ```rust,no_run
/// use td_rredis::{Commands, PipelineCommands};
/// # fn do_something() -> td_rredis::RedisResult<()> {
/// # let client = td_rredis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let key = "the_key";
/// let (new_val,) : (isize,) = try!(td_rredis::transaction(&con, &[key], |pipe| {
///     let old_val : isize = try!(con.get(key));
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key).query(&con)
/// }));
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub fn transaction<K: ToRedisArgs,
                   T: FromRedisValue,
                   F: FnMut(&mut Pipeline) -> RedisResult<Option<T>>>
    (con: &Connection,
     keys: &[K],
     func: F)
     -> RedisResult<T> {
    let mut func = func;
    loop {
        let _: () = try!(cmd("WATCH").arg(keys).query(con));
        let mut p = pipe();
        let response: Option<T> = try!(func(p.atomic()));
        match response {
            None => {
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                let _: () = try!(cmd("UNWATCH").query(con));
                return Ok(response);
            }
        }
    }
}
