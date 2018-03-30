package com.hyo

import org.apache.spark.sql.SparkSession

object QUIZ {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var priceArray = Array(22,33,200,11000,800)
//    var arrayQuiz = priceArray.toString
//    var n = arrayQuiz.substring(1)
//    for( x <- n )
//    print (x)

    var length = priceArray.size
    var a = 0
    for(a <- 0   until length ){
      var tempValue = priceArray(a)
      if(priceArray(a) % 10 == 0)
        println(" "+priceArray(a)+" ")
    }


  }
}
