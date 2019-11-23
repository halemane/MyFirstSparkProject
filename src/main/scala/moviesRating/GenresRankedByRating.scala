package moviesRating

import dataHandler.MovieDataParser
import dataModeler.{MoviesRatingSchema, MoviesRecSchema, UserInfoSchema}
import org.apache.spark.{SparkConf, SparkContext}

object GenresRankedByRating {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //Reading the data from source files
    val userRawData=sc.textFile("src/resources/users.dat")
    val rawData=sc.textFile("src/resources/ratings.dat")
    val moviesData=sc.textFile("src/resources/movies.dat")

    //------------parsing users data----------------//
    val userParsedRec= userRawData.map(line=> {
      val record=MovieDataParser.userRecParser(line)
      if(record.isRight)
        (true,record.right.get)
      else
        (false,line)
    })
val usrNrmlRec= userParsedRec.filter(t=> t._1==true).map(t=> t._2 match{
  case x:UserInfoSchema => x
})
val usrInfoData= usrNrmlRec.map(obj=> (obj.usrId,(obj.age,obj.occupationId)))

   //----------parsing rating data ------------//
   val parsedRec=rawData.map(line=> {
     val record=MovieDataParser.movieRatingRecParse(line)
     if(record.isRight)
       (true,record.right.get)
     else
       (false,line)
   })
    val rateNormalRec=parsedRec.filter(t=>t._1==true).map(t=>t._2 match{
      case x:MoviesRatingSchema => x
    })
    val ratingData=rateNormalRec.map(obj => (obj.movieId,(obj.usrId,obj.rating)))
   //-----parsing movies Data---------------//
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

    // ------------Joining rating and movies data------------//
    val moviesRawData=moviesNormalRec.map(obj=> (obj.movieId,obj.genre))
    val ratingMoviesJoin= ratingData.join(moviesRawData).map(t=> (t._1,t._2._1._1,t._2._1._2,t._2._2))

    //re arranging the fields to do the next join with user dim data//
    //usrId,movieId,rating,genre//
    val ratingMoviesData= ratingMoviesJoin.map(t=> (t._2,(t._1,t._3,t._4)))
    //ratingMoviesData.foreach(x=>println(x))

    //---------Joining ratingMovies data with users dim data ------//
   val rateMovieUserData=usrInfoData.join(ratingMoviesData)

    //----rearranging the fields as userId,movieId,rating,genre,age,occupation -----//
    val rateMovieUserDataRearrange= rateMovieUserData.map(t=>(t._1,t._2._2._1,t._2._2._2,t._2._2._3,t._2._1._1,t._2._1._2))
    //Filtering the records of user who's age is below 18 and assing age group for each user//
    val rateMovieUserDataRaw=rateMovieUserDataRearrange.filter(t=> t._5>=18).map(t=>(t._1,t._2,t._3,
      {
        val str = t._4
        val splitArr=str.replace("|"," ").split(" ")
        if(splitArr.size>1) splitArr(0)
        else splitArr(0)
      }
      ,{
      if(t._5>=18&t._5<=35) "18-35"
     else if(t._5>35& t._5<=50) "36-50" else "50+"
    },t._6
    ))

    val rateMovieUserDataGrp=rateMovieUserDataRaw.groupBy(t=> (t._4,t._5,t._6)).map(t=> (t._1,t._2.map(t=>(t._3)).sum)).map(t=> (t._1._2,t._1._3,t._1._1,t._2))
    val finalRDD=rateMovieUserDataGrp.groupBy(t=> (t._1,t._2)).map(t=> (t._1,t._2.map(t=> (t._3,t._4))))



  }
}
