# HBase Thrift

This library is like [happybase](https://github.com/python-happybase/happybase), but in rust. It provides a way to interact with [HBase](https://hbase.apache.org/)'s thrift interface 

It provides lite wrappers that make it easier to interact with the generated code from HBase's [Thrift Spec](https://github.com/apache/hbase/blob/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift)

Additionnaly, it provides connection pools through [thrift-pool](https://crates.io/crates/thrift-pool) : see the [pool example](./examples/pool.rs)

Examples are under the [examples](./examples) directory

For a real example, see [vector-http-sink-hbase](https://github.com/midnightexigent/vector-http-sink-hbase) which motivated the creation of this crate

[Documentation](https://docs.rs/hbase-thrift/0.7.5/hbase_thrift/)
