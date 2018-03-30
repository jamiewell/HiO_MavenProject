package com.hyo

import org.apache.spark.sql.SparkSession

object OracleLaoding {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    val Kopo_season_ex_Oracle = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load


    Kopo_season_ex_Oracle.createOrReplaceTempView("K")
    Kopo_season_ex_Oracle.show()

    }
  }
