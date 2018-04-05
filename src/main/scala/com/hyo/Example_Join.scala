package com.hyo
import org.apache.spark.sql.SparkSession


object Example_Join {

  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

    var dataPath = "C:/Spark/bin/data/"
    var mainData = "kopo_channel_seasonality_ex.csv"
    var subData = "kopo_product_mst.csv"

    var mainDataDf = spark.read.format("csv").option("header","true").load(dataPath+mainData)
    var subDataDf = spark.read.format("csv").option("header","true").load(dataPath+subData)

      mainDataDf.createOrReplaceTempView("mainTable")
      subDataDf.createOrReplaceTempView("subTable")


}
