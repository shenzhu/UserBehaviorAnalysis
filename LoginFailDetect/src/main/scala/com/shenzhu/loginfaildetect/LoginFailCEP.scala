package com.shenzhu.loginfaildetect

import com.shenzhu.loginfaildetect.LoginFail.getClass
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

object LoginFailCEP {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/LoginLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val loginEventStream = inputStream
            .map(data => {
                val arr = data.split(",")
                LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
                override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
            })

        // Define pattern
        val loginFailPattern = Pattern
            .begin[LoginEvent]("fail").where(_.eventType == "fail").times(3).consecutive()
            .within(Time.seconds(5))

        // Apply pattern
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

        // Filter data stream
        val loginFailWarningStream = patternStream.select(new LoginFailEventMatch())

        loginFailWarningStream.print()

        env.execute("Login fail with CEP")
    }
}

class LoginFailEventMatch() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
    override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
        val iter = map.get("fail").iterator()
        val firstFailEvent = iter.next()
        val secondFailEvent = iter.next()
        val thirdFailEvent = iter.next()

        LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")
    }
}