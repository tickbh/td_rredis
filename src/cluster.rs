use connection::{ConnectionInfo, Connection, connect, into_connection_info, PubSub, connect_pubsub};
use types::{RedisResult, ErrorKind, FromRedisValue};
use std::collections::{HashMap};
use cmd::{Cmd, Pipeline};

pub struct Cluster {
    addrs : HashMap<String, ConnectionInfo>,
    conns : HashMap<String, Connection>,
    slots : HashMap<u16, String>,
    auths : HashMap<String, String>,
}

impl Cluster {

    pub fn new() -> Cluster {
        Cluster {
            addrs : HashMap::new(),
            conns : HashMap::new(),
            slots : HashMap::new(),
            auths : HashMap::new(),
        }
    }

    pub fn add(&mut self, info: &str) -> RedisResult<()> {
        try!(self.init_connection_info(info));
        Ok(())
    }

    /// Returns a PubSub connection.  A pubsub connection can be used to
    /// listen to messages coming in through the redis publish/subscribe
    /// system.
    ///
    /// Note that redis' pubsub operates across all databases.
    pub fn get_pubsub(&self) -> RedisResult<PubSub> {
        let (_, info) = self.addrs.iter().next().unwrap();
        Ok(try!(connect_pubsub(info)))
    }

    pub fn get_connection(&mut self, addr : Option<String>) -> RedisResult<&Connection> {
        let addr = match addr {
            Some(ref addr) => {
                if !self.addrs.contains_key(addr) {
                    try!(self.init_connection_info(&format!("redis://{}", addr)[..]));
                }
                addr
            },
            None => {
                let (k, _) = self.addrs.iter().next().unwrap();
                k
            }
        };
        if self.conns.contains_key(addr) {
            if !self.conns.get(addr).unwrap().is_work() {
                self.conns.remove(addr);
            } else {
                return Ok(self.conns.get(addr).unwrap());    
            }
        }
        let info = unwrap_or!(self.addrs.get(addr), fail!((ErrorKind::InvalidClientConfig, "not addr exist")));
        let connection = try!(connect(&info));
        self.conns.insert(addr.clone(), connection);
        Ok(self.conns.get(addr).unwrap())
    }

    fn init_connection_info(&mut self, info : &str) -> RedisResult<()> {
        let mut connection = try!(into_connection_info(info));
        let addr = format!("{}:{}", connection.host, connection.port);
        if connection.passwd.is_some() {
            self.auths.insert(addr.clone(), connection.passwd.clone().unwrap());
        } else if self.auths.contains_key(&addr) {
            connection.passwd = self.auths.get(&addr).map(|e| e.clone());
        }
        //now Cluster doesn't support SELECT DB
        connection.db = 0;
        self.addrs.insert(addr, connection);
        Ok(())
    }

    pub fn get_connection_by_slot(&mut self, slot : u16) -> RedisResult<&Connection> {
        let addr = self.slots.get(&slot).map_or(None, |e| Some(e.clone()));
        self.get_connection(addr)
    }

    fn remove_connection_by_slot(&mut self, slot : u16) {
        match self.slots.get(&slot).map_or(None, |e| Some(e.clone())) {
            Some(e) => self.remove_connection_by_addr(e.clone()),
            _ => return (),
        };
    }

    fn remove_connection_by_addr(&mut self, addr : String) {
        let _ = self.conns.remove(&addr);
        let _ = self.addrs.remove(&addr);
    }

    fn op_value<T: FromRedisValue>(&mut self, slot : u16, value : RedisResult<T>) -> Option<RedisResult<T>> {
        match value {
            Ok(val) => { 
                return Some(Ok(val))
            },
            Err(err) => {
                if err.kind() == ErrorKind::ExtensionError && err.extension_error_code().unwrap_or("?") == "MOVED" {
                    let detail = err.extension_error_detail().unwrap_or("?");
                    let mut p = detail.splitn(2, ' ');
                    let ret_slot = unwrap_or!(p.next(), return None).to_string();
                    let ret_slot = unwrap_or!(ret_slot.parse::<u16>().ok(), return None);
                    let addr = unwrap_or!(p.next(), return None).to_string();
                    assert_eq!(slot, ret_slot);
                    self.slots.insert(slot, addr);
                } else if err.kind() == ErrorKind::PatternError {
                    self.remove_connection_by_slot(slot);
                    return Some(Err(err));
                } else {
                    return Some(Err(err));
                }
            }
        }
        return None;
    }

    #[inline]
    pub fn query_cmd<T: FromRedisValue>(&mut self, cmd: &Cmd) -> RedisResult<T> {
        let slot = cmd.get_slot();
        loop {
            let value = 
            {
                let connection = try!(self.get_connection_by_slot(slot));
                cmd.query(connection)
            };
            match self.op_value(slot, value) {
                Some(val) => return val,
                _ => continue,
            }
        }
    }

    #[inline]
    pub fn query_pipe<T: FromRedisValue>(&mut self, pipe: &Pipeline) -> RedisResult<T> {
        let slot = pipe.get_slot();
        loop {
            let value = 
            {
                let connection = try!(self.get_connection_by_slot(slot));
                pipe.query(connection)
            };
            match self.op_value(slot, value) {
                Some(val) => return val,
                _ => continue,
            }
        }
    }
}