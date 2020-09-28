package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object SensorStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream.print()

    env.execute()

  }
}
