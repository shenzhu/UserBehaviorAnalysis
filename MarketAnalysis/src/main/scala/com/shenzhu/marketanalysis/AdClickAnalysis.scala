package com.shenzhu.marketanalysis

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvice(windowEnd: String, provice: String, count: Long)

case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

class AdClickProcessFunc() extends ProcessWindowFunction[AdClickLog, AdClickCountByProvice, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[AdClickLog], out: Collector[AdClickCountByProvice]): Unit = {
        val end = new Timestamp(context.window.getEnd).toString
        val count = elements.size

        out.collect(AdClickCountByProvice(end, key, count))
    }
}

class FilterBlackListResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
    lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
    lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

    override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
        val currCount = countState.value()
        if (currCount == 0) {
            val ts = (ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
            resetTimerTsState.update(ts)
            ctx.timerService().registerProcessingTimeTimer(ts)
        }

        if (currCount >= maxCount) {
            if (!isBlackState.value()) {
                isBlackState.update(true)
                ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(
                    value.userId,
                    value.adId,
                    "Click ad over " + maxCount + " times today."
                ))
            }
            return
        }

        val nextCount = currCount + 1
        countState.update(nextCount)
        out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
        if (timestamp == resetTimerTsState.value()) {
            isBlackState.clear()
            countState.clear()
        }
    }
}

object AdClickAnalysis {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/AdClickLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val adLogStream: DataStream[AdClickLog] = inputStream
            .map(data => {
                val arr = data.split(",")
                AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val filterBlackListStream: DataStream[AdClickLog] = adLogStream
            .keyBy(data => (data.userId, data.adId))
            .process(new FilterBlackListResult(10))

        val resultStream: DataStream[AdClickCountByProvice] = filterBlackListStream
            .keyBy(_.province)
            .timeWindow(Time.hours(1), Time.seconds(5))
            .process(new AdClickProcessFunc)

        resultStream.print("count result")
        filterBlackListStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")

        env.execute("Ad Click by Province")
    }
}
