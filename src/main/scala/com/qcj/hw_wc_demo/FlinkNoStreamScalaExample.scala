package com.qcj.hw_wc_demo

import org.apache.flink.api.scala._

/**
  * 开发Flink的DatSet应用程序demo
  */
object FlinkNoStreamScalaExample {
  def main(args: Array[String]): Unit = {
    // 读取文本路径信息
    val filePath1 = "data/hw_wc_demo/log1.txt"
    val filePath2 = "data/hw_wc_demo/log2.txt"

    //批处理程序，需要创建ExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //加载两份数据，并将两份数据union合并
    val text1 = env.readTextFile(filePath1)
    val text2 = env.readTextFile(filePath2)
    val text = text1.union(text2)

    //计算并打印
    /**
      * 1.解析文本数据
      * 2.过滤性别
      * 3.根据姓名和性别分组
      * 4.每组数据累加
      * 5.过滤时间超过120小时的数据
      * 6.打印
      */
    text.map(getRecord(_)).filter(_.sexy == "female").groupBy((e)=>(e.name,e.sexy))
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime>120)
      .print()
  }

  // 解析文本行数据，构造UserRecord数据结构
  def getRecord(line: String): UserRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    UserRecord(name, sexy, time)
  }

  // UserRecord数据结构的定义
  case class UserRecord(name: String, sexy: String, shoppingTime: Int)

}
