package com.shenzhu.marketanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.util.UUID
import scala.util.Random

case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

class SimulatedSource extends RichSourceFunction[MarketUserBehavior] {
    private var running = true
    private var behaviorSeq = Seq("view", "download", "install", "uninstall")
    private var channelSeq = Seq("appstore", "weibo", "wechat", "tieba")

    private var rand = Random

    override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
        val maxCounts = 10000
        var count = 0L

        while (running && count < maxCounts) {
            val id = UUID.randomUUID().toString
            val behavior = behaviorSeq(rand.nextInt(behaviorSeq.size))
            val channel = channelSeq(rand.nextInt(channelSeq.size))
            val ts = System.currentTimeMillis()

            ctx.collect(MarketUserBehavior(id, behavior, channel, ts))

            count += 1
            Thread.sleep(50L)
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}

class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
        val start = new Timestamp(context.window.getStart).toString
        val end = new Timestamp(context.window.getEnd).toString
        val channel = key._1
        val behavior = key._2
        val count = elements.size
        out.collect(MarketViewCount(start, end, channel, behavior, count))
    }
}

object AppMarketByChannel {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val dataStream = env.addSource(new SimulatedSource())
            .assignAscendingTimestamps(_.timestamp)

        val resultStream = dataStream
            .filter(_.behavior != "uninstall")
            .keyBy(data => (data.channel, data.behavior))
            .timeWindow(Time.days(1), Time.seconds(5))
            .process(new MarketCountByChannel)

        resultStream.print()

        env.execute("App Market by Channel")
    }
}
