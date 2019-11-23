package moviesRating

import dataHandler.MovieDataParser
import dataModeler.{MoviesRatingSchema, MoviesRecSchema}
import org.apache.spark.{SparkConf, SparkContext}

object TopTenViewed {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName(getClass.getName).setMaster("local")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawData=sc.textFile("src/resources/ratings.dat")
    val moviesData=sc.textFile("src/resources/movies.dat")
    val parsedRec=rawData.map(line=> {
      val record=MovieDataParser.movieRatingRecParse(line)
      if(record.isRight)
        (true,record.right.get)
      else
        (false,line)
    })
    val normalRec=parsedRec.filter(t=>t._1==true).map(t=>t._2 match{
      case x:MoviesRatingSchema => x
    })
    val moviesParsedRec=moviesData.map(line=> {
      val record=MovieDataParser.moviesRecParse(line)
      if(record.isRight)
        (true,record.right.get)
      else
        (false,line)
    })
    val moviesNormalRec=moviesParsedRec.filter(t=> t._1==true).map(t=> t._2 match{
      case x:MoviesRecSchema => x
    })

    val movieRatingData= normalRec.map(obj=> (obj.movieId,obj.rating)).reduceByKey(_+_).sortBy(t=> -t._2).take(10)
    val topTenMovies=sc.parallelize(movieRatingData)

    val moviesDataJoin= moviesNormalRec.map(obj=> (obj.movieId,obj.title))
    val joinRDD=moviesDataJoin.join(topTenMovies).sortBy(t=> -t._2._2)
    joinRDD.foreach(x=>println(x))
  }

}
