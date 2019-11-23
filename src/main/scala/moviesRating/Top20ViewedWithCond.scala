package moviesRating

import dataHandler.MovieDataParser
import dataModeler.{MoviesRatingSchema, MoviesRecSchema}
import moviesRating.TopTenViewed.getClass
import org.apache.spark.{SparkConf, SparkContext}

object Top20ViewedWithCond {
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
      val moviesDataJoin= moviesNormalRec.map(obj=> (obj.movieId,obj.title))
      val moviesRatingData= normalRec.map(obj=>(obj.movieId,obj.usrId,obj.rating))
      val groupedData=sc.parallelize(moviesRatingData.groupBy(t=> t._1).map(t=>(t._1,t._2.map(t=> (t._2,t._3)))).filter(t=> t._2.size>=40).map(t=> (t._1,t._2.map(t=> t._2).sum)).sortBy(t=> -t._2).take(20))
      val top2OviewedMovies= groupedData.join(moviesDataJoin).sortBy(t=> -t._2._1 )
      top2OviewedMovies.foreach(x=>println(x))

    }

}
