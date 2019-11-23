package Learning

import dataHandler.RecordParser
import dataModeler.ApplySchema
import org.apache.spark.{SparkConf, SparkContext}

object CustomerWiseSalesAmount {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawData=sc.textFile("src/resources/purchase_data.csv")
    val parsedRecord= rawData.map(rec => {
     val record=RecordParser.parse(rec)
      if(record.isRight) (true,record.right.get)
      else(false,rec)
    })

val normalRecord= parsedRecord.filter(t=>t._1==true).map(t=> t._2 match {
  case x:ApplySchema => x
})
    println("good records")
    normalRecord.foreach(println)
val malforamedRecord = parsedRecord.filter(t=>t._1==false).map(t=> t._2)
    println("bad records")
    malforamedRecord.foreach(println)
   val groupedData= normalRecord.map(
     rec => (rec.custId.toInt,rec.amt.toDouble)).reduceByKey(_+_).sortBy(t => t._1)
   println("customerwise sales amount")
    groupedData.foreach(println)
   val custrawData=sc.textFile("src/resources/customers.csv")
    val custKVdata=custrawData.map(rec=> {
      val r=rec.split(",")
      (r(0).toInt,r(1))
    })
    custKVdata.foreach(x=>println(x))
    val joinData=custKVdata.join(groupedData)
    val finalData=joinData.map(t=>(t._1,t._2._1,t._2._2)).sortBy(t=>t._1)
    finalData.foreach(println)

   }

}
