package com.atguigu.day04

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 21点06分
 *
 **/
object WindowWatermark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
    stream
      .map {
        r => {
          val strings = r.split(" ")
          (strings(0), strings(1).toLong * 1000L)
        }
      }
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
          })
      )
      .keyBy(r => r._1)
      .timeWindow(Time.seconds(5))
      .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
          var count =0L
          for (elem <- elements) {
            count+=1
          }
          out.collect("当前窗口中的元素个数为:"+count)
        }
      })
        .print()

    env.execute()
  }
}
