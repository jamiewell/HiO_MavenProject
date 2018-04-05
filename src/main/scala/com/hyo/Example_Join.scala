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

    spark.sql("select a.regionid, a.product, b.productname, a.yearweek, a.qty " +
    "from mainTable a " +
    "left join subTable b " +
    "on a.product = b.productid" )

//    Load two datatables from oracle and activate method "join"

  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
// 데이터 파일 호출의 변수'명'은 고정속성이 아님...
  var seasonalityTableName = "kopo_channel_seasonality_new"
  var regionMstTableName = "kopo_region_mst"

  val selloutDb_new_Oracle = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> seasonalityTableName, "user" -> staticUser, "password" -> staticPw)).load

  val selloutDb_mst_Oracle = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> regionMstTableName, "user" -> staticUser, "password" -> staticPw)).load

  selloutDb_new_Oracle.createOrReplaceTempView("SEASONALITY_NEW")
  selloutDb_new_Oracle.show()

  selloutDb_mst_Oracle.createOrReplaceTempView("REGION_MST")
  selloutDb_mst_Oracle.show()

  val LeftResult = spark.sql("select  A.REGIONID, B.REGIONNAME, A.PRODUCT, A.YEARWEEK, A.QTY " + "from SEASONALITY_NEW A " + "left join REGION_MST B " + "on A.REGIONID = B.REGIONID" )

  val InnerResult = spark.sql("select b.regionname, a.regionid,  a.product, a.yearweek, a.qty   " +
    "from SEASONALITY_NEW A " +
    "inner join REGION_MST B " +
    "on a.regionid = b.regionid" )

  val RightResult = spark.sql("select a.regionid, b.regionname,  a.product, a.yearweek, a.qty " +
    "from seasonality_new a " +
    "right join region_mst b " +
    "on a.regionid = b.regionid" )

  spark.sql("select * from " + ("select a.regionid,  a.product, a.yearweek, a.qty, b.regionname "
    + "from kopo_channel_seasonality_new a + "
    + "left join kopo_region_mst b "
    + "on a.regionid = b.regionid " )
    + "where 1=1 and regionid = 'A77' ")


}
