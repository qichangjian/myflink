package com.qcj.hw_wc_demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * 开发Flink的DataStream应用程序demo
  *
  * 思路：参数解析
  * filePath为文本读取路径，用逗号分隔。
  * windowTime;为统计数据的窗口跨度,时间单位都是分。
  *
  *
  * 网址：
  * ParameterTool介绍：https://www.jianshu.com/p/a71b0ed7ef15
  * EventTime时间戳提取器： https://blog.csdn.net/xianpanjia4616/article/details/84971274
  *                         https://blog.csdn.net/xu470438000/article/details/83271123
  *
  *
  * 滚动时间窗口：          http://www.cnblogs.com/felixzh/p/9698073.html
  */
object FlinkStreamJavaExample {
  def main(args: Array[String]): Unit = {

    // 打印出执行flink run的参考命令
    System.out.println("use command as: ")
    System.out.println("./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkStreamScalaExample /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2")
    System.out.println("******************************************************************************************")
    System.out.println("<filePath> is for text file to read data, use comma to separate")
    System.out.println("<windowTime> is the width of the window, time as minutes")
    System.out.println("******************************************************************************************")

    // 读取文本路径信息，并使用逗号分隔
    val filePaths = ParameterTool.fromArgs(args).get("filePath",
      "data/hw_wc_demo/log1.txt,data/hw_wc_demo/log2.txt").split(",").map(_.trim)
    assert(filePaths.length > 0) // 断言路径不为空

    // windowTime设置窗口时间大小，默认2分钟一个窗口足够读取文本内的所有数据了
    val windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2)

    //StreamExecutionEnvironment 执行流程序的上下文。环境提供了控制作业执行的方法（例如设置并行性或容错/检查点参数）以及与外部世界交互（数据访问）。
    // 构造执行环境，使用eventTime处理窗口数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置使用eventtime，默认是使用processtime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度为1,默认并行度是当前机器的cpu数量
    env.setParallelism(1)

    // 读取文本数据流
    val unionStream = if (filePaths.length > 1) {
      val firstStream = env.readTextFile(filePaths.apply(0))//取出第一个路径加载数据
      firstStream.union(filePaths.drop(1).map(it => env.readTextFile(it)): _*) //drop: drop(n: Int): List[A] 丢弃前n个元素，返回剩下的元素
    } else {
      env.readTextFile(filePaths.apply(0))
    }

    // 数据转换，构造整个数据处理的逻辑，计算并得出结果打印出来
    unionStream.map(getRecord(_)) //// 将读入的字符串转化为 UserRecord 对象
      .assignTimestampsAndWatermarks(new Record2TimestampExtractor)
      .filter(_.sexy == "female")//过滤famal
      .keyBy("name", "sexy")//使用name和sexy
      .window(TumblingEventTimeWindows.of(Time.minutes(windowTime))) //滚动事件时间窗口(tumbling event-time windows)
      .reduce((e1, e2) => UserRecord(e1.name, e1.sexy, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // 调用execute触发执行
    env.execute("FemaleInfoCollectionPrint scala")
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


  // 构造继承AssignerWithPunctuatedWatermarks的类，用于设置eventTime以及waterMark
  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[UserRecord] {
    // add tag in the data of datastream elements在datastream元素的数据中添加标记
    override def extractTimestamp(element: UserRecord, previousTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready 给水印触发窗口执行，并使用该值检查窗口元素是否准备就绪
    def checkAndGetNextWatermark(lastElement: UserRecord,extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }
  }
}
