package com.atguigu.day04

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/30 00点07分
 *
 **/
object RedirectLateEventToSideOutputExample2 {
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
      .process(new KeyedProcessFunction[String, (String, Long), String] {
        override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
          if (value._2 < ctx.timerService().currentWatermark()) {
            ctx.output(new OutputTag[String]("last-data"), "时间戳为" + value._1 + "的数据迟到！")
          } else {
            out.collect("时间戳为：" + value._2 + "的数据没有迟到")
          }
        }
      })
    process.print()
    process.getSideOutput(new OutputTag[String]("last-data")).print()


    env.execute()
  }
}
