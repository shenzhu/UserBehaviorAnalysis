package com.shenzhu.networkflowanalysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)


class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
        out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
    }
}

class TopNHotPages(size: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

    lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
    lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

    override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
        pageViewCountMapState.put(value.url, value.count)

        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        if (timestamp == ctx.getCurrentKey + 60000L) {
            pageViewCountMapState.clear()

            return
        }

        val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
        val iter = pageViewCountMapState.entries().iterator()
        while (iter.hasNext) {
            val entry = iter.next()
            allPageViewCounts += ((entry.getKey, entry.getValue))
        }

        val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(size)

        val result: StringBuilder = new StringBuilder
        result.append("Window End Time：").append( new Timestamp(timestamp - 1) ).append("\n")

        for (i <- sortedPageViewCounts.indices){
            val currentItemViewCount = sortedPageViewCounts(i)
            result.append("NO.").append(i + 1).append(": \t")
                .append("Page URL = ").append(currentItemViewCount._1).append("\t")
                .append("Hot = ").append(currentItemViewCount._2).append("\n")
        }

        result.append("\n==================================\n\n")

        Thread.sleep(1000)

        out.collect(result.toString())
    }
}


object HotPagesNetworkFlow {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val inputStream: DataStream[String] = env.readTextFile("/Users/shenzhu/Desktop/Github/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log")

        val dataStream: DataStream[ApacheLogEvent] = inputStream
            .map(data => {
                val arr = data.split(" ")

                val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val ts = simpleDateFormat.parse(arr(3)).getTime

                ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
                override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
            })

        val aggStream = dataStream
            .filter(_.method == "GET")
            .keyBy(_.url)
            .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
            .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

        val resultStream = aggStream
            .keyBy(_.windowEnd)
            .process(new TopNHotPages(3))

        aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late data")
        resultStream.print().setParallelism(1)

        env.execute("hot pages job")
    }
}
