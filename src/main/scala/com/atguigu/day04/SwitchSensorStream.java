package com.atguigu.day04;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;



/**
 * 王林
 * 2020/9/29 09点16分
 **/
public class SwitchSensorStream {
    public static void main(String[] args) throws  Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<SensorReading, String> stream = env.addSource(new SensorSource()).keyBy(r -> r.id());

        KeyedStream<Tuple2<String, Long>, String> switchStream = env.fromElements(Tuple2.of("sensor_1", 10 * 1000L)).keyBy(r -> r.f0);

        stream
                .connect(switchStream)
                .process(new MyCoProcessFunC())
                .print();

        env.execute();
    }

    public static class MyCoProcessFunC extends CoProcessFunction<SensorReading,Tuple2<String,Long>,SensorReading>{
        private ValueState<Boolean> theSwitch ;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            theSwitch = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN));
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if(theSwitch.value()!=null && theSwitch.value()){
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            theSwitch.update(true);
            long theTimer = ctx.timerService().currentProcessingTime() + value.f1;
            ctx.timerService().registerProcessingTimeTimer(theTimer);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            theSwitch.clear();
        }
    }

}
