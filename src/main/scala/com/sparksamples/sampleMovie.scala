package com.sparksamples


import scala.language.implicitConversions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}



object sampleMovie extends Serializable{
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)
  case class MovieSchema(sno:Option[Int],movie_name:String,release_year:String,rating:String,duration:Option[Int]);
  //case class MovieSchema(sno:String,movie_name:String,release_year:String,rating:String,duration:String);
  def main(args:Array[String]):Unit= {

    logger.info("starting code")
    val spark=SparkSession.builder().master("local[3]").appName("Movie Dataset").getOrCreate()
    import spark.implicits._
    val movieInfo = spark.sparkContext.textFile("./src/main/resources/Dataset_movie.txt");

    val movieDF =movieInfo.map(_.split(",")).filter(_.length==5).map(r => MovieSchema(Some(r(0).toInt),r(1),r(2),r(3),Some(r(4).toInt))).toDF()
    //val locDF =Car_Info.map(_.split(",")).map(r => MovieSchema(r(0),r(1),r(2),r(3),r(4))).toDF()

    //Registering the DataFrame as a temporary view
    movieDF.createOrReplaceTempView("movieData")
    //locDF.show()
    movieDF.printSchema()

    //The total number of movies
    val movieCount=spark.sql("SELECT COUNT(*) FROM movieData").collect()
    //records.show()

    // The maximum rating of movies
    val maxRating=spark.sql("SELECT max(rating) FROM movieData").collect()

    //The number of movies that have maximum rating
    val movieNum=spark.sql("SELECT count(*) FROM movieData where rating=(SELECT max(rating) FROM movieData)").collect()

    // The movies with ratings 1 and 2
    val movie1Or2=spark.sql("SELECT movie_name FROM movieData where rating=='1' or rating=='2'").collect()

    //The list of years and number of movies released each year
    val numMoviePerYear=spark.sql("SELECT release_year,count(movie_name) FROM movieData GROUP BY release_year").collect()

   //The number of movies that have a runtime of two hours
    val movieRunTime=spark.sql("SELECT count(movie_name) FROM movieData where duration==7200").collect()


    print(" The total number of movies are ")
    movieCount.foreach(x=>println(x(0)))
    print("  The maximum rating of movies is ")
    maxRating.foreach(x=>println(x(0)))
    print("  The number of movies that have maximum rating are ")
    movieNum.foreach(x=>println(x(0)))
    println("  The movies with ratings 1 and 2 are ")
    movie1Or2.foreach(x=>println(x(0)))
    println("  The list of years and number of movies released each year are ")
    numMoviePerYear.foreach(println)
    print("  The number of movies that have a runtime of two hours are ")
    movieRunTime.foreach(x=>println(x(0)))
    scala.io.StdIn.readLine()
    spark.stop()

  }

}





