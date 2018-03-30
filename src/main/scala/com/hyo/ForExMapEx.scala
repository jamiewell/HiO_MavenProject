package com.hyo

import org.apache.spark.sql.SparkSession

object ForExMapEx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size

    for(i <-0 until priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
    }

    var priceData2 = Array(1000.0,1200.0,1300.0,1500.0,10000.0)

      priceData2.map(x=>{
        x - (x*promotionRate)
      })


  }
}
