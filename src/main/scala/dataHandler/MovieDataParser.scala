package dataHandler

import dataModeler.{MoviesRatingSchema, MoviesRecSchema, UserInfoSchema}
import javax.lang.model.element.NestingKind

object MovieDataParser {

  def movieRatingRecParse(rec:String):Either[Exception,MoviesRatingSchema]={
    val splitArr=rec.split("::")
    if(splitArr.size==4)
      {
        val mro=MoviesRatingSchema(splitArr(0).toInt,splitArr(1).toInt,splitArr(2).toInt,splitArr(3))
        val r=Right(mro)
        r
      }
    else Left(new Exception)
  }
  def moviesRecParse(rec:String):Either[Exception,MoviesRecSchema]={
    val splitArr=rec.split("::")
    if(splitArr.size==3)
      {
        val mro=MoviesRecSchema(splitArr(0).toInt,splitArr(1),splitArr(2))
        val r=Right(mro)
        r
      }
    else
      Left(new Exception)
  }

  def userRecParser(rec:String):Either[Exception,UserInfoSchema]={
    val splitArr=rec.split("::")
    if(splitArr.size==5 )
    {
      val mro=UserInfoSchema(splitArr(0).toInt,splitArr(1),splitArr(2).toInt,splitArr(3).toInt,splitArr(4))
      val r=Right(mro)
      r
    }
    else
      Left(new Exception)

  }

}
