package studentData

import dataHandler.StdRecParser
import dataModeler.StdRecord
import org.apache.spark.{SparkConf, SparkContext}

object StudentTotalMarks {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val badRecCount=sc.accumulator(0)
    val rawStdData=sc.textFile("src/resources/student_marks.csv")
    val parsedData=rawStdData.map(rec=> {
      val records = StdRecParser.parse(rec)
      if(records.isRight) (true,records.right.get)
      else (false,rec)
    })
    val normalRecords=parsedData.filter(t => t._1==true).map(t=> t._2 match {
      case x:StdRecord => x
    })
    val stdRec= normalRecords.map(obj => (obj.stdId,(obj.marks,obj.standard)))
    val grpData=stdRec.reduceByKey((x,y)=>((x._1+y._1,x._2)))
    val stdRawData=scala.io.Source.fromFile("src/resources/student_data.csv").getLines()
    val stdMap=stdRawData.map(rec => rec.split(",")).map(arr=> (arr(0).toInt,(arr(1),arr(2)))).toMap
    //println(stdMap.getOrElse(1,"unknown"))
    val stdBroadCast=sc.broadcast(stdMap)

    val lkpData=grpData.map(rec =>{
      val id= rec._1
      val name=stdBroadCast.value.getOrElse(id,("unknown","unknown"))
      (name,rec)
    })

val finalOutPut=lkpData.map(t=> (t._2._1,t._1._1,t._1._2,t._2._2._2,t._2._2._1)).sortBy(t=> - t._5)

finalOutPut.foreach(println)

    println("bad records")
    val badRec=parsedData.filter(t=> t._1==false).map(t=> t._2)
    badRec.foreach(x=>println(x))
  }

}
