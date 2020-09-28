package com.atguigu.day02

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    stream
        .map(new RichMapFunction[Int,Int] {
          override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            println("声明周期开始")
          }

          override def map(value: Int): Int = {
            value+1
          }

          override def close(): Unit = {
            super.close()
            println("生命周期结束")
          }
        })
        .print()

    env.execute()
  }
}
