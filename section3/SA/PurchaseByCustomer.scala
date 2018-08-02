package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Sum up how many each customer has spent */
object PurchaseByCustomer 
{
  
/** A function that splits a line of input into (customerID, spend) tuples. */
  def parseLine(line: String) = 
  {
      // Split by commas
      val fields = line.split(",")
      // Extract the customerID and spend fields, and convert to integers
      val customerID = fields(0).toInt
      val spend = fields(2).toFloat
      // Create a tuple that is our result.
      (customerID, spend)
  }
  
/** Our main function where the action happens */
  def main(args: Array[String]) 
  {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../customer-orders.csv")
    
    // Use our parseLines function to convert to (customerID, spend) tuples
    val rdd = lines.map(parseLine)
    
    // Sum up the total spend for customerID
    val totalsByCustomer = rdd.reduceByKey( (x,y) => (x + y))
    
    // Flip (customerID, spend) tuples to (spend,customerID) and then sort by key (the spend)
    val totalsSorted = totalsByCustomer.map( x => (x._2, x._1) ).sortByKey()
    
    // Collect the results from the RDD
    val results = totalsSorted.collect()
    
    // Print the results, flipping the (spend,customerID) results to customerID: spend as we go.
    for (result <- totalsSorted) 
    {
      val spend = result._1
      val customerID = result._2
      val formattedSpend = f"$spend%.2f GBP"
      println(s"$customerID: $formattedSpend")
    } 
  } 
}