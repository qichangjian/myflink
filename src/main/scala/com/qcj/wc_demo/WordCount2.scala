package com.qcj.wc_demo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 实时接收word count例子
  *
  * 运行准备
  * 1.windows上安装netcat，下载解压，配置路径path就行了:
  *   参考博客：  https://blog.csdn.net/qq_37585545/article/details/82250984
  * 2.运行前先cmd启动nc:就可以输入数据了
  *   nc -lL -p 9999
  * 3.之后运行此程序，记住要先启动nc.后运行程序，不然会报错
  */
object WordCount2 {
  def main(args: Array[String]) {
    /*
    在Flink程序中首先需要创建一个StreamExecutionEnvironment
    （如果你在编写的是批处理程序，需要创建ExecutionEnvironment），它被用来设置运行参数。
    当从外部系统读取数据的时候，它也被用来创建源（sources）。
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }//nonEmpty非空的
      .map { (_, 1) }
      .keyBy(0)//通过Tuple的第一个元素进行分组
      .timeWindow(Time.seconds(5))//Windows 根据某些特性将每个key的数据进行分组 (例如:在5秒内到达的数据).
      .sum(1)

    //将结果流在终端输出
    counts.print
    //开始执行计算
    env.execute("Window Stream WordCount")
  }
}
