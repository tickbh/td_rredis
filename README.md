# redis-rs

td_rredis is a high level redis library for Rust.  It provides convenient access
to all Redis functionality through a very flexible but low-level API.  It
uses a customizable type conversion trait so that any operation can return
results in just the type you are expecting.  This makes for a very pleasant
development experience. if you redis in cluster, it will auto find new server,
and get the data right in the server.

## Basic Operation

To open a connection you need to create a cluster and then to fetch a
connection from it.  In the future there will be a connection pool for
those, currently each connection is separate and not pooled.

Many commands are implemented through the `Commands` trait but manual
command creation is also possible.

if you deploy the redis cluster on 127.0.0.1:7000-127.0.0.1:7006, you can add 127.0.0.1:7000, it will discover other server
```rust
extern crate td_rredis as redis;

fn test_cluster() {
    let mut cluster = redis::Cluster::new();
    for i in 7000 .. 7001 {
        cluster.add(&format!("redis://127.0.0.1:{}/", i)[..]).unwrap();    
    }

    let _ : () = redis::cmd("set").arg("xxoo1").arg("ooxx").query_cluster(&mut cluster).unwrap();
    assert_eq!(redis::cmd("get").arg("xxoo1").query_cluster(&mut cluster), Ok("ooxx".to_string()));
}
```
