package com.atguigu.day02.util

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
class SensorSource extends RichParallelSourceFunction[SensorReading]{
  var running =true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random

    var curFTemp = (1 to 10).map(
      i=>("sensor_"+i,rand.nextGaussian()*20)
    )
    while (running){
      curFTemp=curFTemp.map(
        t=>(t._1,t._2+rand.nextGaussian()*0.5)
      )
      val curTime = Calendar.getInstance.getTimeInMillis
      curFTemp.foreach(t=>sourceContext.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}
