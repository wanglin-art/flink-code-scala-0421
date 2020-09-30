package com.atguigu.day04

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 23点52分
 *
 **/
object RedirectLateEventToSideOutputExample1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')
    val operator: DataStream[(String, Long)] = stream
      .map(r => (r.split(" ")(0), r.split(" ")(1).toLong * 1000L))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
          })
      )
    val process: DataStream[String] = operator
      .keyBy(r => r._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(new OutputTag[(String, Long)]("last-data"))
      .process(new ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
          var count = 0L
          for (elem <- elements) {
            count += 1
          }
          out.collect("当前窗口中一共有：" + count + "条数据")
        }
      })
    process.print()

    process.getSideOutput(new OutputTag[(String,Long)]("last-data")).print()

    env.execute()

  }
}
