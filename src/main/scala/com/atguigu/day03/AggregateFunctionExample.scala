package com.atguigu.day03

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 * 王林
 * 2020/9/28 19点41分
 *
 *
 **/
object AggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
        .map(new MapFunction[SensorReading,(String, Double)] {
          override def map(value: SensorReading): (String, Double) = {
            (value.id,value.temperature)
          }
        })
        .keyBy(r=>r._1)
        .timeWindow(Time.seconds(5))
        .aggregate(new MinAgg)
        .print()
    env.execute()
  }

  class MinAgg extends AggregateFunction[(String,Double),(String,Double),(String,Double)]{
    override def createAccumulator(): (String, Double) = {
      ("",Double.MaxValue)
    }

    override def add(value: (String, Double), accumulator: (String, Double)): (String, Double) = {
      if(value._2<accumulator._2){
        value
      }else{
        accumulator
      }
    }

    override def getResult(accumulator: (String, Double)): (String, Double) = {
      accumulator
    }

    override def merge(a: (String, Double), b: (String, Double)): (String, Double) = {
      null
    }
  }
}
