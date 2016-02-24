use connection::{ConnectionInfo, Connection, connect, into_connection_info, PubSub, connect_pubsub};
use types::{RedisResult};

/// The client type.
#[derive(Debug, Clone)]
pub struct Client {
    connection_info: ConnectionInfo,
}

/// The client acts as connector to the redis server.  By itself it does not
/// do much other than providing a convenient way to fetch a connection from
/// it.  In the future the plan is to provide a connection pool in the client.
///
/// When opening a client a URL in the following format should be used:
///
/// ```plain
/// redis://host:port/db
/// ```
///
/// Example usage::
///
/// ```rust,no_run
/// let client = td_rredis::Client::open("redis://127.0.0.1/").unwrap();
/// let con = client.get_connection().unwrap();
/// ```
impl Client {

    /// Connects to a redis server and returns a client.  This does not
    /// actually open a connection yet but it does perform some basic
    /// checks on the URL that might make the operation fail.
    pub fn open(info: &str) -> RedisResult<Client> {
        Ok(Client {
            connection_info: try!(into_connection_info(info))
        })
    }

    /// Instructs the client to actually connect to redis and returns a
    /// connection object.  The connection object can be used to send
    /// commands to the server.  This can fail with a variety of errors
    /// (like unreachable host) so it's important that you handle those
    /// errors.
    pub fn get_connection(&self) -> RedisResult<Connection> {
        Ok(try!(connect(&self.connection_info)))
    }

    /// Returns a PubSub connection.  A pubsub connection can be used to
    /// listen to messages coming in through the redis publish/subscribe
    /// system.
    ///
    /// Note that redis' pubsub operates across all databases.
    pub fn get_pubsub(&self) -> RedisResult<PubSub> {
        Ok(try!(connect_pubsub(&self.connection_info)))
    }
}
