import dataHandler.SocialFriendsParser
import dataModeler.SocialFriendsSchema
import org.apache.spark.{SparkConf, SparkContext}

object SocialFriendsAgeWiseAvg {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc= new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawDataRDD= sc.textFile("src/resources/social_friends.csv")
    val parsedRecords=rawDataRDD.map(line => {
      val record=SocialFriendsParser.parse(line)
      if(record.isRight)
        (true,record.right.get)
      else
        (false,line)
    })
    val normalRecRDD=parsedRecords.filter(t=> t._1==true).map(t=> t._2 match{
      case x:SocialFriendsSchema => x
    })
   val sfRDD=normalRecRDD.map(obj => (obj.usrAge,obj.numOfFrnds))
   val countRDD=sfRDD.groupBy(t=> t._1).map(t=> (t._1,t._2.map(t=> t._2)))
   val avgFrndsAgewise=countRDD.map(t=>(t._1,
    (t._2.sum/t._2.size).toDouble
    ))
  avgFrndsAgewise.sortBy(t=> -t._2).foreach(x=>println(x))
  }
}
