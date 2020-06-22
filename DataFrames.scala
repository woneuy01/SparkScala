package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
    
object DataFrames {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val lines = spark.sparkContext.textFile("../fakefriends.csv")
    val people = lines.map(mapper).toDS().cache()
    
    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    people.printSchema()
    
    println("Let's select the name column:")
    people.select("name").show()
    
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
   
    println("Group by age:")
    people.groupBy("age").count().show()
    
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }
}
==================================================================
Here is our inferred schema:
root
 |-- ID: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = false)
 |-- numFriends: integer (nullable = false)

Let's select the name column:
+--------+
|    name|
+--------+
|    Will|
|Jean-Luc|
|    Hugh|
|  Deanna|
|   Quark|
|  Weyoun|
|  Gowron|
|    Will|
|  Jadzia|
|    Hugh|
|     Odo|
|     Ben|
|   Keiko|
|Jean-Luc|
|    Hugh|
|     Rom|
|  Weyoun|
|     Odo|
|Jean-Luc|
|  Geordi|
+--------+
only showing top 20 rows

Filter out anyone over 21:
+---+-------+---+----------+
| ID|   name|age|numFriends|
+---+-------+---+----------+
| 21|  Miles| 19|       268|
| 48|    Nog| 20|         1|
| 52|Beverly| 19|       269|
| 54|  Brunt| 19|         5|
| 60| Geordi| 20|       100|
| 73|  Brunt| 20|       384|
|106|Beverly| 18|       499|
|115|  Dukat| 18|       397|
|133|  Quark| 19|       265|
|136|   Will| 19|       335|
|225|   Elim| 19|       106|
|304|   Will| 19|       404|
|327| Julian| 20|        63|
|341|   Data| 18|       326|
|349| Kasidy| 20|       277|
|366|  Keiko| 19|       119|
|373|  Quark| 19|       272|
|377|Beverly| 18|       418|
|404| Kasidy| 18|        24|
|409|    Nog| 19|       267|
+---+-------+---+----------+
only showing top 20 rows

Group by age:
+---+-----+
|age|count|
+---+-----+
| 31|    8|
| 65|    5|
| 53|    7|
| 34|    6|
| 28|   10|
| 26|   17|
| 27|    8|
| 44|   12|
| 22|    7|
| 47|    9|
| 52|   11|
| 40|   17|
| 20|    5|
| 57|   12|
| 54|   13|
| 48|   10|
| 19|   11|
| 64|   12|
| 41|    9|
| 43|    7|
+---+-----+
only showing top 20 rows

Make everyone 10 years older:
+--------+----------+
|    name|(age + 10)|
+--------+----------+
|    Will|        43|
|Jean-Luc|        36|
|    Hugh|        65|
|  Deanna|        50|
|   Quark|        78|
|  Weyoun|        69|
|  Gowron|        47|
|    Will|        64|
|  Jadzia|        48|
|    Hugh|        37|
|     Odo|        63|
|     Ben|        67|
|   Keiko|        64|
|Jean-Luc|        66|
|    Hugh|        53|
|     Rom|        46|
|  Weyoun|        32|
|     Odo|        45|
|Jean-Luc|        55|
|  Geordi|        70|
+--------+----------+
only showing top 20 rows

