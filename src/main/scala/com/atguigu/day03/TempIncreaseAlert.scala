package com.atguigu.day03

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/28 22点27分
 *
 **/
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream
      .keyBy(r => r.id)
      .process(new MyKeyedFunC)
      .print()
    env.execute()
  }

  class MyKeyedFunC extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )

    // 默认值是0L
    lazy val timer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )


    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      val preTemp = lastTemp.value()

      lastTemp.update(value.temperature)

      val aTimer = timer.value()


      if (preTemp == 0.0 || value.temperature < preTemp) {
        ctx.timerService().deleteEventTimeTimer(aTimer)
        println(value.id+",       "+value.temperature)
        timer.clear()
      } else if (value.temperature > preTemp && aTimer == 0) {
        val thisTime: Long = ctx.timerService().currentProcessingTime() + 2000L
        ctx.timerService().registerProcessingTimeTimer(thisTime)
        timer.update(thisTime)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("警报！ID为：" + ctx.getCurrentKey + "的传感器温度持续1S上升")
      timer.clear()
    }
  }

}
