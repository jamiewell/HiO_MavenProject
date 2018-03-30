package com.hyo

import org.apache.spark.sql.SparkSession

object Example_02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.2:1522/XE"
    var staticUser = "hyo"
    var staticPw = "hyo2"
    var selloutDb = "kopo_product_volume"

    // jdbc (java database connectivity) 연결
    val selloutDataFromToad= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromToad.createOrReplaceTempView("volumeTable")
    selloutDataFromToad.show




  }
}
