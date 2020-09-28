package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._


/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object MapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream
      .map(r=>r.id)
    //        .print()
    stream
      .map(new MapFunction[SensorReading,String] {
          override def map(t: SensorReading): String = {
            return t.id
          }
        })
    //        .print()
    stream
      .map(new MyMap)
      .print()
    env.execute()
  }
  class MyMap extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = {
      return t.id
    }
  }
}
