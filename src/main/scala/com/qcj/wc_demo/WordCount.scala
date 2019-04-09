package com.qcj.wc_demo

import org.apache.flink.api.scala._

/**
  * 简单的批处理word count例子
  */
object WordCount {
  def main(args: Array[String]) {
    //批处理程序，需要创建ExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromElements(elements:_*) --- 从一个给定的对象序列中创建一个数据流，所有的对象必须是相同类型的。
    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?","hah")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)//根据第一个元素分组
      .sum(1)

    //打印
    counts.print()
  }
}
