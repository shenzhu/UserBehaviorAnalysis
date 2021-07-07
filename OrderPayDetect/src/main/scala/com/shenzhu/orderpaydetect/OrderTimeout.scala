package com.shenzhu.orderpaydetect

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.util

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
        val timeoutOrderId = map.get("create").iterator().next().orderId
        OrderResult(timeoutOrderId, "timeout " + ": +" + l)
    }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
        val payedOrderid = map.get("pay").iterator().next().orderId
        OrderResult(payedOrderid, "payed successfully")
    }
}

object OrderTimeout {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resource = getClass.getResource("/OrderLog.csv")
        val inputStream = env.readTextFile(resource.getPath)

        val orderEventStream = inputStream
            .map(data => {
                val arr = data.split(",")
                OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000L)

        val orderPayPattern = Pattern
            .begin[OrderEvent]("create").where(_.eventType == "create")
            .followedBy("pay").where(_.eventType == "pay")
            .within(Time.minutes(15))

        val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

        val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

        val resultStream = patternStream.select(
            orderTimeoutOutputTag,
            new OrderTimeoutSelect(),
            new OrderPaySelect()
        )

        resultStream.print("Payed")
        resultStream.getSideOutput(orderTimeoutOutputTag).print("Timeout")

        env.execute("Order timeout job")
    }
}
