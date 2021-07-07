package com.shenzhu.networkflowanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
case class PvCount(windowEnd: Long, count: Long)

class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
        out.collect(PvCount(window.getEnd, input.head))
    }
}

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount] {
    lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

    override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
        totalPvCountResultState.update(totalPvCountResultState.value() + value.count)

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
        val totalPvCount = totalPvCountResultState.value()
        totalPvCountResultState.clear()

        out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    }
}

object PageView {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // Read from input
        val resource = getClass.getResource("/UserBehavior.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val pvStream: DataStream[PvCount] = dataStream
            .filter(_.behavior == "pv")
            .map(data => (Random.nextString(10), 1L))
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .aggregate(new PvCountAgg(), new PvCountWindowResult())

        val totalPvStream = pvStream
            .keyBy(_.windowEnd)
            .process(new TotalPvCountResult())

        totalPvStream.print()

        env.execute("PV Count")
    }
}
