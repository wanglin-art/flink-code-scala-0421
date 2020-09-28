package com.atguigu.day03

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

/**
 * 王林
 * 2020/9/28 20点31分
 *
 **/
object AvgTempWithWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
        .keyBy(r=>r.id)
        .timeWindow(Time.seconds(5))
        .process(new MyProcessAvg)
        .print()
    env.execute()
  }
  class MyProcessAvg extends ProcessWindowFunction[SensorReading,(String,Double),String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[(String, Double)]): Unit = {
      var sum =0.0
      var count =0L
      for (elem <- elements) {
        sum+=elem.temperature
        count+=1
      }
      out.collect("传感器的ID为："+key+"的5秒内的平均温度为：",sum/count)
    }
  }

}
