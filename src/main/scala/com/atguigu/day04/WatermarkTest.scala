package com.atguigu.day04

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 20点41分
 * 为乱序时间流插入水位线
 **/
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("localhost", 9999,'\n')
    stream
      .map(r => (r.split(" ")(0), r.split(" ")(1).toLong * 1000L))
      .assignAscendingTimestamps(r => r._2)
      .keyBy(r => r._1)
      .process(new MyProFun)
      .print()

    env.execute()
  }

  class MyProFun extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      out.collect("当前的水位线是：" + ctx.timerService().currentWatermark())
    }
  }

}
