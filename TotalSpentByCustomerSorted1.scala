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
========================================================
(45,3309.3804)
(79,3790.5698)
(96,3924.23)
(23,4042.65)
(99,4172.29)
(75,4178.5)
(36,4278.05)
(98,4297.26)
(47,4316.3)
(77,4327.7305)
(13,4367.62)
(48,4384.3296)
(49,4394.5996)
(94,4475.5703)
(67,4505.79)
(50,4517.2695)
(78,4524.51)
(5,4561.0703)
(57,4628.3994)
(83,4635.8003)
(91,4642.2603)
(74,4647.1304)
(84,4652.9395)
(3,4659.63)
(12,4664.59)
(66,4681.92)
(56,4701.02)
(21,4707.41)
(80,4727.86)
(14,4735.0303)
(37,4735.2)
(7,4755.0693)
(44,4756.8906)
(31,4765.05)
(82,4812.49)
(4,4815.05)
(10,4819.6997)
(88,4830.55)
(20,4836.86)
(89,4851.4795)
(95,4876.8394)
(38,4898.461)
(76,4904.2104)
(86,4908.809)
(27,4915.8896)
(18,4921.27)
(53,4945.3)
(1,4958.5996)
(51,4975.2197)
(16,4979.0605)
(30,4990.72)
(28,5000.7104)
(22,5019.449)
(29,5032.5303)
(17,5032.6797)
(60,5040.7095)
(25,5057.6104)
(19,5059.4307)
(81,5112.71)
(69,5123.01)
(65,5140.3496)
(11,5152.29)
(35,5155.42)
(40,5186.4297)
(87,5206.3994)
(52,5245.0605)
(26,5250.4004)
(62,5253.3213)
(33,5254.659)
(24,5259.92)
(93,5265.75)
(64,5288.69)
(90,5290.41)
(55,5298.09)
(9,5322.6494)
(34,5330.7993)
(72,5337.4395)
(70,5368.2505)
(43,5368.83)
(92,5379.281)
(6,5397.8794)
(15,5413.5103)
(63,5415.15)
(58,5437.7305)
(32,5496.0503)
(61,5497.48)
(85,5503.4307)
(8,5517.24)
(0,5524.9497)
(41,5637.619)
(59,5642.8906)
(42,5696.8403)
(46,5963.111)
(97,5977.1895)
(2,5994.591)
(71,5995.66)
(54,6065.39)
(39,6193.1104)
(73,6206.199)
(68,6375.45)


