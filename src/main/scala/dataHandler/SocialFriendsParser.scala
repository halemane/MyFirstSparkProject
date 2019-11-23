package dataHandler

import dataModeler.SocialFriendsSchema

object SocialFriendsParser {

  def parse(rec:String):Either[Exception,SocialFriendsSchema]={
    val splitArr=rec.split(",")
    if(splitArr.size==4)
      {
        val sfr= SocialFriendsSchema(splitArr(0).toInt,splitArr(1),splitArr(2).toInt,splitArr(3).toInt)
        val r=Right(sfr)
        r
      }
    else Left(new Exception)
  }

}
