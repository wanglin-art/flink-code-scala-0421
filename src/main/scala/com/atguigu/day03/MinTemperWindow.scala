package com.atguigu.day03

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 *
 * 王林
 * 2020/9/28 19 22
 *
 *
 **/
object MinTemperWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream
        .map(r=>(r.id,r.temperature))
        .keyBy(r=>r._1)
        .timeWindow(Time.seconds(5))
        .reduce((r1,r2)=>(r1._1,Math.min(r1._2,r2._2)))
        .print()

    env.execute()
  }
}
