
extern crate url;
extern crate sha1;
// #![macro_use]
macro_rules! ensure {
    ($expr:expr, $err_result:expr) => (
        if !($expr) { return $err_result; }
    )
}

macro_rules! fail {
    ($expr:expr) => (
        return Err(::std::convert::From::from($expr));
    )
}

macro_rules! unwrap_or {
    ($expr:expr, $or:expr) => (
        match $expr {
            Some(x) => x,
            None => { $or; }
        }
    )
}


mod types;
mod client;
mod connection;
mod parser;
mod cmd;
mod commands;
mod script;
mod slot;
mod cluster;

/* public api */
pub use parser::{parse_redis_value, Parser};
pub use client::Client;
pub use connection::{Connection, ConnectionInfo, PubSub, Msg, transaction};
pub use cmd::{cmd, Cmd, pipe, Pipeline, Iter, pack_command};
pub use commands::{Commands, PipelineCommands};
pub use script::{Script, ScriptInvocation};
pub use cluster::Cluster;
pub use slot::key_hash_slot;
#[doc(hidden)]
pub use types::{
    /* low level values */
    Value,

    /* error and result types */
    RedisError,
    RedisResult,

    /* error kinds */
    ErrorKind,

    /* utility types */
    InfoDict,
    NumericBehavior,

    /* conversion traits */
    FromRedisValue,
    ToRedisArgs,

    /* utility functions */
    from_redis_value,
    no_connection_error,
    make_extension_error,
};
