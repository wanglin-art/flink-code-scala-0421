package com.atguigu.day04

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Duration

import com.sun.org.glassfish.external.statistics.TimeStatistic
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 22点13分
 *
 **/
object EventTimeTimer {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')
    stream
        .map(r=>(r.split(" ")(0),r.split(" ")(1).toLong * 1000L))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy
            .forBoundedOutOfOrderness[(String,Long)](Duration.ofSeconds(5))
            .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
              override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
            })
        )
        .keyBy(r=>r._1)
        .process(new MyProcessFun)
        .print()
    env.execute()
  }
  class MyProcessFun extends KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      ctx.timerService().registerEventTimeTimer(value._2+ 10*1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      out.collect("时间戳为："+new Timestamp(timestamp)+"的定时器触发了！")
    }
  }
}
