package com.atguigu.day02.util

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
 *
 * 王林
 * 2020/9/27
 *
 *
 **/
class SmokeSource extends RichParallelSourceFunction[SmokeLevel]{
    var running = true

  override def run(sourceContext: SourceFunction.SourceContext[SmokeLevel]): Unit = {
   val rand = new Random()

    while (running){
      if(rand.nextGaussian()>0){
        sourceContext.collect(new SmokeLevel("high"))
      }else{
        sourceContext.collect(new SmokeLevel("low"))
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running=false
  }

}
