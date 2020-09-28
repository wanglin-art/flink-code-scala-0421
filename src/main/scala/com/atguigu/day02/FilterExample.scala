package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object FilterExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
        .filter(t=>t.id.equals("sensor_1"))
//        .print()

    stream
        .filter(new FilterFunction[SensorReading] {
          override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
        })
//        .print()
    stream
        .filter(new MyFilter)
        .print()

    env.execute()
  }
  class MyFilter extends FilterFunction[SensorReading]{
    override def filter(value: SensorReading): Boolean = value.id.equals("sensor_1")
  }
}
