package com.shenzhu.hotitemanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
;


case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId = key.asInstanceOf[Tuple1[Long]].f0
        val windowEnd = window.getEnd
        val count = input.iterator.next()

        out.collect(ItemViewCount(itemId, windowEnd, count))
    }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var itemViewCountListState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        itemViewCountListState.add(value)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()

        itemViewCountListState.get().forEach(item => allItemViewCounts += item)
        itemViewCountListState.clear()

        val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

        val result: StringBuilder = new StringBuilder
        result.append("Window End Time: ").append(new Timestamp(timestamp - 1)).append("\n")

        for (i <- sortedItemViewCounts.indices) {
            val currItemViewCount = sortedItemViewCounts(i)
            result.append("NO.").append(i + 1).append(": \t")
                .append("Item ID = ").append(currItemViewCount.itemId).append("\t")
                .append("Hot = ").append(currItemViewCount.count).append("\n")
        }

        result.append("\n============================\n\n")

        Thread.sleep(1000)

        out.collect(result.toString())
    }
}


object HotItems {
    def main(args: Array[String]): Unit = {
        // Set environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // Read input
        val inputStream: DataStream[String] = env.readTextFile("/Users/shenzhu/Documents/Projects/UserBehaviorAnalysis/HotItemAnalysis/src/main/resources/UserBehavior.csv")

        // Data transform
        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        // Aggregate every window
        val aggStream: DataStream[ItemViewCount] = dataStream
            .filter(_.behavior == "pv")
            .keyBy("itemId")
            .timeWindow(Time.hours(1), Time.minutes(5))
            .aggregate(new CountAgg(), new ItemViewWindowResult())

        val resultStream: DataStream[String] = aggStream
            .keyBy("windowEnd")
            .process(new TopNHotItems(5))
        resultStream.print()

        env.execute("hot items")
    }
}
