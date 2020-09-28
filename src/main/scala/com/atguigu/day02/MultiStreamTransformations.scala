package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource, SmokeLevel, SmokeSource}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object MultiStreamTransformations {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorStream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val smokeStream: DataStream[SmokeLevel] = env.addSource(new SmokeSource).setParallelism(1)

    sensorStream
        .connect(smokeStream.broadcast)
        .flatMap(new CoFlatMapFunction[SensorReading,SmokeLevel,Alert] {
          private var level: SmokeLevel = new SmokeLevel("low")
          override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
            if (in1.temperature>20 && level.stats.equals("high")){
              collector.collect(new Alert("WARN!"+in1,in1.timestamp))
            }
          }
          override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
            level.stats = in2.stats
          }
        })
        .print()
    env.execute()
  }



}
//class MyCoFlatMapFunction extends CoFlatMapFunction[SensorReading,SmokeLevel,Alert]{
//  private var level: SmokeLevel = new SmokeLevel("low")
//
//  override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
//      if (in1.temperature>60 && level.stats.equals("high")){
//        collector.collect(new Alert("WARNING!!!!"+in1,in1.timestamp))
//      }
//  }
//
//  override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
//        level.stats = in2.stats
//  }
//}
