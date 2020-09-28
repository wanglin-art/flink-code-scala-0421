package com.atguigu.day02

import com.atguigu.day02.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
object ReduceExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream
        .filter(t=>t.id.equals("sensor_1"))
        .map(t=>(t.id,t.temperature))
        .keyBy(t=>t._1)
        .reduce((t1,t2)=>(t1._1,t1._2.max(t2._2)))
        .print()


//    stream
//        .filter(t=>t.id.equals("sensor_1"))
//        .map{
//          t=>{
//            (t.id,t.temperature)
//          }
//        }
//        .keyBy(t=>t._1)
//        .reduce(new ReduceFunction[(String, Double)] {
//          override def reduce(value1: (String, Double), value2: (String, Double)): (String, Double) = {
//            if(value1._2>value2._2){
//              return value1
//            }else{
//              return value2
//            }
//          }
//        })
//        .print()
    env.execute()
  }
}
