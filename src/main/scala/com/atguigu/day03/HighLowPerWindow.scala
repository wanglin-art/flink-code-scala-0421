package com.atguigu.day03



import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.atguigu.day02.util.{SensorReading, SensorSource}
import com.atguigu.day03.util.HighLowTemp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * 王林
 * 2020/9/28 20点55分
 *
 **/
object HighLowPerWindow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream
        .keyBy(r=>r.id)
        .timeWindow(Time.seconds(5))
        .aggregate(new MyAgg,new MyProcess)
        .print()
    env.execute()
  }
  class MyAgg extends AggregateFunction[SensorReading,(String,Double,Double),(String,Double,Double)]{
    override def createAccumulator(): (String, Double, Double) = {
      ("",Double.MinValue,Double.MaxValue)
    }

    override def add(r: SensorReading, acc: (String, Double, Double)): (String, Double, Double) = {
      (r.id,Math.max(r.temperature,acc._2),Math.min(r.temperature,acc._3))
    }

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = {
      accumulator
    }

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = null
  }

  class MyProcess extends ProcessWindowFunction[(String,Double,Double),HighLowTemp,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[HighLowTemp]): Unit = {
      val thisOne: (String, Double, Double) = elements.toIterator.next()

      val startTm: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getStart)
      val endTm: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.window.getEnd)
      out.collect(new HighLowTemp(key,thisOne._2,thisOne._3,startTm,endTm))
    }
  }
}
