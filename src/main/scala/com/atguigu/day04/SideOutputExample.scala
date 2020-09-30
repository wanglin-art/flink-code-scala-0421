package com.atguigu.day04

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/29 23点31分
 *
 **/
object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    var outputTag1 = new OutputTag[String]("greatThan80")

//    val process: DataStream[SensorReading] = stream
//      .keyBy(r => r.id)
//      .process(new KeyedProcessFunction[String, SensorReading, SensorReading] {
//        override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
//          if (value.temperature > 80) {
//            ctx.output(outputTag1, "id为：" + value.id + "的温度感应器温度超过80°，目前是：" + value.temperature)
//          }
//        }
//      })

    val process2: DataStream[SensorReading] =  stream
      .process(new ProcessFunction[SensorReading, SensorReading] {
        override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
          if (value.temperature < 80) {
            ctx.output(outputTag1, "id为：" + value.id + "的温度感应器温度小于80°，目前是：" + value.temperature)
          }
        }
      })

    process2.getSideOutput(outputTag1).print()

    env.execute()
  }
}
