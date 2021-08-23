package homework5


import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("InvertedIndex")
    conf.setMaster("local")

    val sparkContext = new SparkContext(conf)
    val data = sparkContext.wholeTextFiles("src/main/resources/files")

    val r1 = data.flatMap { x =>
      //使用分割"/''获取文件名
      val doc = x._1.split(s"/").last.split("\\.").head
      //在按空格进行切分行
      x._2.split("\r\n").flatMap(_.split(" ").map { y => (y+","+ doc,1)})
    }

      .reduceByKey((a,b)=>a+b).map(y => (y._1.split(",").head, (y._1.split(",").last,y._2)) ).groupByKey().map(y=>(y._1+":{"+y._2.toList.mkString(",")+"}"))

    r1.foreach(println)


    r1.saveAsTextFile("src/main/resources/InvertedIndex")
  }

}