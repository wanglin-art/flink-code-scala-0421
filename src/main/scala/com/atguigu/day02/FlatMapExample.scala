package com.atguigu.day02

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.fromElements("white", "black", "black", "gray")
    stream
        .flatMap(new FlatMapFunction[String,String] {
          override def flatMap(value: String, out: Collector[String]): Unit = {
            if (value.equals("white")){
              out.collect(value)
            }else if(value.equals("black")){
              out.collect(value)
              out.collect(value)
            }
          }
        })
//        .print()
    stream
        .flatMap(new MyFlatMap)
        .print()

    env.execute()
  }
  class MyFlatMap extends FlatMapFunction[String,String]{
    override def flatMap(value: String, out: Collector[String]): Unit = {
      if (value.equals("white")){
        out.collect(value)
      }else if(value.equals("black")){
        out.collect(value)
        out.collect(value)
      }
    }
  }
}
