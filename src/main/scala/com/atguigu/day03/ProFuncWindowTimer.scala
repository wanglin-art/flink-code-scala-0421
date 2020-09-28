package com.atguigu.day03


import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._

import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/28 21点17分
 *
 **/
object ProFuncWindowTimer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999)
    stream
      .map(new MapFunction[String, (String, String)] {
        override def map(value: String): (String, String) = {
          val strings: Array[String] = value.split(" ")
          (strings(0), strings(1))
        }
      })
      .keyBy(r => r._1)
      .process(new MyTimer)
      .print()
    env.execute()
  }

  class MyTimer extends KeyedProcessFunction[String, (String, String), String] {
    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      val timeThis: Long = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timeThis)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      out.collect("警报！！！")
    }
  }

}
