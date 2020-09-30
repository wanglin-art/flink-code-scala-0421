package com.atguigu.day04

import java.sql.Timestamp
import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 22点28分
 *
 **/
object PeriodicWatermarkGenerator {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')
    stream
      .map(r => (r.split(" ")(0), r.split(" ")(1).toLong * 1000L))
      .assignTimestampsAndWatermarks(
        new AssignerWithPeriodicWatermarks[(String, Long)] {
          var bound = 5 * 1000L
          var maxTm = Long.MinValue + bound + 1

          override def getCurrentWatermark: Watermark = {
                new Watermark(maxTm-bound-1)
          }

          override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
            maxTm= Math.max(element._2,maxTm)
            element._2
          }
        }
      )
        .keyBy(r=>r._1)
        .process(new KeyedProcessFunction[String,(String,Long),String] {
          override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
            out.collect("当前的水位线是："+ctx.timerService().currentWatermark())
            ctx.timerService().registerEventTimeTimer(value._2+ 10 *1000L)
          }

          override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
            super.onTimer(timestamp, ctx, out)
            out.collect("时间戳为："+new Timestamp(timestamp)+"的定时器触发了！！！")
          }
        })
        .print()
    //        .assignTimestampsAndWatermarks(
    //          WatermarkStrategy
    //            .forBoundedOutOfOrderness[(String,Long)](Duration.ofSeconds(5))
    //            .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
    //              override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = {
    //                element._2
    //              }
    //            })
    //        )

    env.execute()
  }
}
