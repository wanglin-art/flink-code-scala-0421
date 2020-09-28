package com.atguigu.day03

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 王林
 * 2020/9/28 20点14分
 *
 **/
object AvgTempPerWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new MyAvg)
      .print()
    env.execute()
  }
  class MyAvg extends AggregateFunction[SensorReading, (String, Double, Long), (String, Double)] {
    override def createAccumulator(): (String, Double, Long) = {
      ("",0.0,0L)
    }

    override def add(r: SensorReading, acc: (String, Double, Long)): (String, Double, Long) = {
      (r.id,acc._2+r.temperature,acc._3+1)
    }

    override def getResult(accumulator: (String, Double, Long)): (String, Double) = {
      (accumulator._1,accumulator._2/accumulator._3)
    }

    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = null
  }


}

