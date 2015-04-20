# RxSpark

RxSpark is a wrapper for Spark Streaming. This library was developed as part of the course IN4389 (Reactive Programming)
at Delft University of Technology. It provides the following features:

- support for using an `Observable` as input to a Spark Streaming application;
- support for using the output of the Spark Streaming application as an `Observable`;
- put backpressure on the `Observable` if the cluster is too slow;

## References

For more information on Spark or Spark Streaming, see [spark.apache.org](http://spark.apache.org). For more information
on Reactive Extensions, see [reactivex.io](http://reactivex.io).
