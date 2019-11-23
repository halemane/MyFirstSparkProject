package dataHandler

import dataModeler.ApplySchema

object RecordParser {
  def parse(arr:String):Either[Exception,ApplySchema]={
    val splitArr=arr.split(",")
    if (splitArr.size==4) {
      val tranId= splitArr(0)
      val custId= splitArr(1)
      val prodId= splitArr(2)
      val amt= splitArr(3)
      val rc= ApplySchema(tranId, custId, prodId, amt)
      val r=Right(rc)
      r
    }
    else Left(new Exception())
  }
}
