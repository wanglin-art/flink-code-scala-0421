package com.atguigu.day01

import org.apache.flink.streaming.api.scala._

/**
 *
 * @author 王林
 *
 **/
object WordCountFromSocket {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("hadoop102", 9999)
    stream.flatMap(r=>r.split(" "))
        .map(r=>(r,1))
        .keyBy(r=>r._1)
        .sum(1)
        .print()

    env.execute()
  }
}
