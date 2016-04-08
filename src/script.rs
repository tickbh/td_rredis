use std::io::prelude::*;
use std::fs::File;

use sha1::Sha1;

use cmd::cmd;
use types::{ToRedisArgs, FromRedisValue, RedisResult, ErrorKind};
use connection::ConnectionLike;

/// Represents a lua script.
#[derive(Debug)]
pub struct Script {
    code: String,
    hash: String,
    path: String,
}

/// The script object represents a lua script that can be executed on the
/// redis server.  The object itself takes care of automatic uploading and
/// execution.  The script object itself can be shared and is immutable.
///
/// Example:
///
/// ```rust,no_run
/// # let client = td_rredis::Client::open("redis://127.0.0.1/").unwrap();
/// # let con = client.get_connection().unwrap();
/// let script = td_rredis::Script::new(r"
///     return tonumber(ARGV[1]) + tonumber(ARGV[2]);
/// ");
/// let result = script.arg(1).arg(2).invoke(&con);
/// assert_eq!(result, Ok(3));
/// ```
impl Script {
    /// Creates a new script object.
    pub fn new(code: &str) -> Script {
        let mut hash = Sha1::new();
        hash.update(code.as_bytes());
        Script {
            code: code.to_string(),
            hash: hash.hexdigest(),
            path: String::new(),
        }
    }

    pub fn new_hash(hash: &str) -> Script {
        Script {
            code: String::new(),
            hash: hash.to_string(),
            path: String::new(),
        }
    }

    pub fn new_path_hash(path: &str, hash: &str) -> RedisResult<Script> {
        if hash.len() == 0 {
            let mut f = try!(File::open(path));
            let mut code = String::new();
            try!(f.read_to_string(&mut code));

            let mut hash = Sha1::new();
            hash.update(code.as_bytes());
            Ok(Script {
                code: code,
                hash: hash.hexdigest(),
                path: String::new(),
            })
        } else {
            Ok(Script {
                code: String::new(),
                hash: hash.to_string(),
                path: path.to_string(),
            })
        }
    }


    /// Returns the script's SHA1 hash in hexadecimal format.
    pub fn get_hash(&self) -> &str {
        &self.hash
    }

    /// Creates a script invocation object with a key filled in.
    #[inline]
    pub fn key<T: ToRedisArgs>(&self, key: T) -> ScriptInvocation {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: key.to_redis_args(),
        }
    }

    /// Creates a script invocation object with an argument filled in.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&self, arg: T) -> ScriptInvocation {
        ScriptInvocation {
            script: self,
            args: arg.to_redis_args(),
            keys: vec![],
        }
    }

    /// Returns an empty script invocation object.  This is primarily useful
    /// for programmatically adding arguments and keys because the type will
    /// not change.  Normally you can use `arg` and `key` directly.
    #[inline]
    pub fn prepare_invoke(&self) -> ScriptInvocation {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }
    }

    /// Invokes the script directly without arguments.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &ConnectionLike) -> RedisResult<T> {
        ScriptInvocation {
            script: self,
            args: vec![],
            keys: vec![],
        }
        .invoke(con)
    }
}

/// Represents a prepared script call.
pub struct ScriptInvocation<'a> {
    script: &'a Script,
    args: Vec<Vec<u8>>,
    keys: Vec<Vec<u8>>,
}

/// This type collects keys and other arguments for the script so that it
/// can be then invoked.  While the `Script` type itself holds the script,
/// the `ScriptInvocation` holds the arguments that should be invoked until
/// it's sent to the server.
impl<'a> ScriptInvocation<'a> {
    /// Adds a regular argument to the invocation.  This ends up as `ARGV[i]`
    /// in the script.
    #[inline]
    pub fn arg<T: ToRedisArgs>(&'a mut self, arg: T) -> &'a mut ScriptInvocation {
        self.args.extend(arg.to_redis_args().into_iter());
        self
    }

    /// Adds a key argument to the invocation.  This ends up as `KEYS[i]`
    /// in the script.
    #[inline]
    pub fn key<T: ToRedisArgs>(&'a mut self, key: T) -> &'a mut ScriptInvocation {
        self.keys.extend(key.to_redis_args().into_iter());
        self
    }

    /// Invokes the script and returns the result.
    #[inline]
    pub fn invoke<T: FromRedisValue>(&self, con: &ConnectionLike) -> RedisResult<T> {
        loop {
            match cmd("EVALSHA")
                      .arg(self.script.hash.as_bytes())
                      .arg(self.keys.len())
                      .arg(&*self.keys)
                      .arg(&*self.args)
                      .query(con) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(err) => {
                    // May only has hash but not code
                    if err.kind() == ErrorKind::NoScriptError && self.script.code.len() > 0 {
                        let hash: String = try!(cmd("SCRIPT")
                                                    .arg("LOAD")
                                                    .arg(self.script.code.as_bytes())
                                                    .query(con));
                        ensure!(self.script.hash == hash,
                                fail!((ErrorKind::BusyLoadingError, "load hash error")));
                    } else if err.kind() == ErrorKind::NoScriptError && self.script.path.len() > 0 {
                        let mut f = try!(File::open(&*self.script.path));
                        let mut code = String::new();
                        try!(f.read_to_string(&mut code));

                        let hash: String = try!(cmd("SCRIPT")
                                                    .arg("LOAD")
                                                    .arg(code.as_bytes())
                                                    .query(con));
                        ensure!(self.script.hash == hash,
                                fail!((ErrorKind::BusyLoadingError, "load hash error")));
                    } else {
                        fail!(err);
                    }
                }
            }
        }
    }
}
