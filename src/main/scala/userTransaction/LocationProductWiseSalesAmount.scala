package userTransaction

import dataHandler.{RecordParser, TranRecParser}
import dataModeler.TranRecSchema
import org.apache.spark.{SparkConf, SparkContext}

object LocationProductWiseSalesAmount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName(getClass.getName).setMaster("local")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val tranRawRDD=sc.textFile("src/resources/tran_info.csv")
    val parsedRecRDD= tranRawRDD.map(rec =>{
      val record=TranRecParser.parse(rec)
      if(record.isRight)
        (true,record.right.get)
      else
        (false,rec)
    })
val normaRecRDD= parsedRecRDD.filter(rec=> rec._1==true).map(rec => rec._2 match {
  case x:TranRecSchema => x
})
 val salesRDD=normaRecRDD.map(obj=> (obj.custId,(obj.prdId,obj.amt)))
 val custInfo=scala.io.Source.fromFile("src/resources/user_info.csv").getLines()
 val custData=custInfo.map(rec=> rec.split(" ")).map(arr=> (arr(0).toInt,arr(3))).toMap
 val brodcastVar=sc.broadcast(custData)
val joinRDD=salesRDD.map(t => {
  val custId = t._1
  val loc=brodcastVar.value.getOrElse(custId,"unkown")
  ((loc,t._2._1),t._2._2)
})

    val finalRDD=joinRDD.reduceByKey(_+_)
    finalRDD.saveAsTextFile("src/resources/output")

  }
}
