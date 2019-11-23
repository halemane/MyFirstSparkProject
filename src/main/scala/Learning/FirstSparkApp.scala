package Learning

import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApp {

  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("first spark app")
      .setMaster("local")
    val sc= new SparkContext(conf)
    val rawRDD=sc.textFile("C:\\Users\\p.hanumantharayapp\\Desktop\\Training\\demoText.txt")
    val wordRDD= rawRDD.flatMap(line => line.split(" "))
    val kvWordRDD= wordRDD.map(w => (w,1))
    val grpRDD= kvWordRDD.groupByKey()
    val wordCountRDD= grpRDD.map(t => (t._1,t._2.sum))
    wordCountRDD.foreach(println)

  }

}
