package com.shenzhu.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd: Long, count: Long)

class UvCountResult() extends ProcessAllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        var userIdSet = Set[Long]()

        elements.foreach(userBehavior => userIdSet += userBehavior.userId)

        out.collect(UvCount(context.window.getEnd, userIdSet.size))
    }
}

class UvCountResultApply() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        // 定义一个Set
        var userIdSet = Set[Long]()

        // 遍历窗口中的所有数据，把userId添加到set中，自动去重
        for( userBehavior <- input )
            userIdSet += userBehavior.userId

        // 将set的size作为去重后的uv值输出
        out.collect(UvCount(window.getEnd, userIdSet.size))
    }
}

object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/UserBehavior.csv")
        val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

        val dataStream: DataStream[UserBehavior] = inputStream
            .map(data => {
                val arr = data.split(",")
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val uvStream = dataStream
            .filter(_.behavior == "pv")
            .timeWindowAll(Time.hours(1))
            .process(new UvCountResult())

        uvStream.print()

        env.execute("UV Count Job")
    }
}
