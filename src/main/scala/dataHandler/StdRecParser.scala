package dataHandler

import dataModeler.StdRecord

object StdRecParser {
    def parse(rec:String):Either[Exception,StdRecord]={
      val stdArr=rec.split(",")
      if(stdArr.size==4)
        {
          val id=stdArr(0).toInt
          val sub=stdArr(1)
          val marks=stdArr(2).toInt
          val cls=stdArr(3)
          val sr=StdRecord(id,sub,marks,cls)
          val r=Right(sr)
          r
        }
      else {
        Left(new Exception)
      }
    }
}
