package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

/** Find the movies with the most ratings. */
object PopularMoviesNicer {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("ISO-8859-1") // This is the current encoding of u.item, not UTF-8.

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")  
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
  }
  
}
===========================================
(Mostro, Il (1994),1)
(Coldblooded (1995),1)
(Nemesis 2: Nebula (1995),1)
(Silence of the Palace, The (Saimt el Qusur) (1994),1)
(Land and Freedom (Tierra y libertad) (1995),1)
(Walk in the Sun, A (1945),1)
(Tainted (1998),1)
(Homage (1995),1)
(Mat' i syn (1997),1)
(Mamma Roma (1962),1)
(Man from Down Under, The (1943),1)
(Scream of Stone (Schrei aus Stein) (1991),1)
(Sliding Doors (1998),1)
(The Courtyard (1995),1)
(Hush (1998),1)
(Farmer & Chase (1995),1)
(Window to Paris (1994),1)
(JLG/JLG - autoportrait de décembre (1994),1)
(Deceiver (1997),1)
(Shadow of Angels (Schatten der Engel) (1976),1)
(Jupiter's Wife (1994),1)
(Normal Life (1996),1)
(Small Faces (1995),1)
(Eye of Vichy, The (Oeil de Vichy, L') (1993),1)
(Hungarian Fairy Tale, A (1987),1)
(Nobody Loves Me (Keiner liebt mich) (1994),1)
(King of New York (1990),1)
(Liebelei (1933),1)
(Eighth Day, The (1996),1)
(Desert Winds (1995),1)
(Other Voices, Other Rooms (1997),1)
(Gate of Heavenly Peace, The (1995),1)
(Quartier Mozart (1992),1)
(Chairman of the Board (1998),1)
(Girl in the Cadillac (1995),1)
(Brothers in Trouble (1995),1)
.
.
.
.
.
....................................
