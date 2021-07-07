package com.shenzhu.loginfaildetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, waringMsg: String)

class LoginFailWarningResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
    lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
        if (value.eventType == "fail") {
            val iter = loginFailListState.get().iterator()

            // Check if we have failed before
            if (iter.hasNext) {
                val firstFailEvent = iter.next()
                if (value.timestamp < firstFailEvent.timestamp + 2) {
                    out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
                }

                loginFailListState.clear()
                loginFailListState.add(value)
            } else {
                loginFailListState.add(value)
            }
        } else {
            loginFailListState.clear()
        }
    }
}

object LoginFail {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/LoginLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val loginEventStream = inputStream
            .map( data => {
                val arr = data.split(",")
                LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            } )
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
            })

        val loginFailWarningStream = loginEventStream
            .keyBy(_.userId)
            .process(new LoginFailWarningResult())

        loginFailWarningStream.print()

        env.execute("Login fail detect")
    }
}
