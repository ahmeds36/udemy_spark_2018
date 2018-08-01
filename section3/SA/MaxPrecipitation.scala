package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the maximum precipitation by day for a year */
object MaxPrecipitation {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val day = fields(1)
    val entryType = fields(2)
    val precipitation = fields(3).toFloat
    (day, entryType, precipitation)
  }
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPrecipitation")
    
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxPrecip = parsedLines.filter(x => x._2 == "PRCP")
    val dayPrecip = maxPrecip.map(x => (x._1, x._3.toFloat))
    val maxPrecipByDay = dayPrecip.reduceByKey( (x,y) => max(x,y))
    val results = maxPrecipByDay.collect()
    
    for (result <- results.sorted) {
       val day = result._1
       val precip = result._2
       val formattedPrecip = f"$precip%.2f units"
       println(s"$day max precipitation: $formattedPrecip") 
    }
      
  }
}