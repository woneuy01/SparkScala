package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the total amount spent per customer in some fake e-commerce data. */
object TotalSpentByCustomerSorted1 {
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")   
    
    val input = sc.textFile("../customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)
    
    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val totalByCustomerSorted = totalByCustomer.map( x => (x._2, x._1) ).sortByKey()
    
    val flipped=totalByCustomerSorted.map(x=>(x._2,x._1))
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = flipped.collect()
    
    // Sort and print the final results.
    results.foreach(println)
  } 
}

