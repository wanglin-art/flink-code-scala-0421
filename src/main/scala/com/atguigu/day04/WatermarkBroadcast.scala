package com.atguigu.day04

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 23点20分
 *
 **/
object WatermarkBroadcast {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1: DataStream[(String, Long)] = env
      .socketTextStream("localhost", 9999, '\n')
      .map(r => (r.split(" ")(0), r.split(" ")(1).toLong * 1000L))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
              element._2
            }
          })
      )

    val stream2: DataStream[(String, Long)] = env
      .socketTextStream("localhost", 9998, '\n')
      .map(r => (r.split(" ")(0), r.split(" ")(1).toLong * 1000L))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
              element._2
            }
          })
      )


    stream1
      .union(stream2)
      .keyBy(r => r._1)
      .process(new MyKeyedFunC2)
      .print()
    env.execute()
  }

  class MyKeyedFunC2 extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect("当前的水位线是：" + ctx.timerService().currentWatermark())
    }
  }

}
