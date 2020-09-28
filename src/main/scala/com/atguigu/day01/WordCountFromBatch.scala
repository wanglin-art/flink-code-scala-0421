package com.atguigu.day01

import org.apache.flink.streaming.api.scala._

/**
 *
 * @author 王林
 *
 **/
object WordCountFromBatch {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.fromElements("hello hello world", "hello world hello")

     stream.flatMap(r => r.split(" "))
      .map(r => (r, 1))
      .keyBy(r => r._1)
      .sum(1)
         .print()

    env.execute()
  }
}
