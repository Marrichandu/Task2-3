package com.sparksamples


import scala.language.implicitConversions
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}


class busBreakdown extends Serializable {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)


  // Most common reasons for either a delay or breaking down of the bus
  def commonReason(df1:DataFrame):DataFrame= {

    df1.select("Reason").filter(df1("Reason")==="Mechanical Problem"|| df1("Reason")==="Flat Tire" ||
      df1("Reason")==="Won`t Start"||df1("Reason")==="Heavy Traffic" )
      .groupBy("Reason").count().withColumnRenamed("count(1)", "count")
      .orderBy(col("count").desc)

  }

  //Top five route numbers where the bus was either delayed or broke down
  def RouteNumbers(df2:DataFrame):DataFrame={
    df2.select("Route_Number").groupBy("Route_Number").count().withColumnRenamed("count(1)", "count")
      .orderBy(col("count").desc).limit(5)
    //df2.withColumn("count",expr("count")/2)
  }
  // The total number of incidents, year-wise, when the students were
  //In Bus
  def AccidentsInBus(df3:DataFrame):DataFrame={
    df3.where("Number_Of_Students_On_The_Bus>0")
    .groupBy("School_Year").count()

  }

  //NotIn Bus
  def AccidentsNotInBus(df4:DataFrame):DataFrame={
    df4.where("Number_Of_Students_On_The_Bus==0")
      .groupBy("School_Year").count()
  }

  //The year in which accidents were less
  def AccidentsMin(df5:DataFrame):DataFrame={
    df5.groupBy("School_Year").count().orderBy(col("count")).limit(1)
  }


  //Loads the dataframe in spark
  def loadSurveyDf(spark:SparkSession, DataFile: String):DataFrame={
     spark.read
     .option("header","true")
     .option("InferSchema","true")
     .csv(DataFile)
  }

  //Set configuration details for spark session

  def getSparkConf:SparkConf={
    val sparkAppConf=new SparkConf()
    //sparkAppConf.set("spark.app.name","sample").set("spark.master","local[3]")
    //sparkAppConf.set("spark.app.name","sample").set("spark.master","local[3]").set("spark.sql.shuffle.partitions","2")
    sparkAppConf.setAppName("sample")
      .setMaster("local[3]")
    sparkAppConf
  }
}
