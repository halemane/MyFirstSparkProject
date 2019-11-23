package dataHandler

import dataModeler.TranRecSchema

object TranRecParser {
  def parse(rec:String):Either[Exception,TranRecSchema]={
    val splitArr= rec.split(" ")
    if(splitArr.size==5)
      {
        val trs=TranRecSchema(splitArr(0).toInt, splitArr(1).toInt, splitArr(2).toInt, splitArr(3).toDouble,splitArr(4))
        val r=Right(trs)
        r
      }
    else {
      Left(new Exception)
    }
  }

}
