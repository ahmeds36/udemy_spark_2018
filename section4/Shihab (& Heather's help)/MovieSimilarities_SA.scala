package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object MovieSimilarities_SA {

/*
 * EXECUTE WITH THE LINE BELOW (NOTE THERE ARE 5 PARAMETERS ENTERES AT THE END OF THE COMMAND):
 * spark-submit --class com.sundogsoftware.spark.MovieSimilarities_SA MovieSims_SA.jar 50 5 0.01 50
 * 50 -> movie id of the movie we're interested in (50 refers to Star Wars, of course)
 * 5 -> excludes all movies below this rating
 * 0.01 -> excludes all matching pair scores below this threshold
 * 50 -> excludes any recommendations below this co-occurence threshold  
 */
  
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  
//SA EDIT #5A START **** Include movie genre ********************  
// Create a Map of Ints to Strings and and array of Ints, and populate it from u.item.

  /** Load up a Map of movie IDs to movie names and genres. */
  def loadMovieNamesGenres() : Map[Int, (String, Array[Int])] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNamesGenres:Map[Int, (String, Array[Int])] = Map()
    
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 5) {
        val movieID : Int = fields(0).toInt
        val movieName : String = fields(1)
        val genres: Array[Int] = fields.drop(5).map(a => a.toInt)
        movieNamesGenres += (movieID -> (movieName, genres))
      }
    }   
    return movieNamesGenres
  }

// Weight the similarity scores by the number of shared genres
  def weightByGenre( movieSimilarlity:((Int, Int), (Double, Int), (Array[Int], Array[Int])) ) : ((Int, Int), (Double, Int)) = {
    val moviePair : (Int, Int) = movieSimilarlity._1
    val score : Double = movieSimilarlity._2._1
    val numPairs : Int = movieSimilarlity._2._2
    val genres1 : Array[Int] = movieSimilarlity._3._1
    val genres2 : Array[Int] = movieSimilarlity._3._2
    
    // generate a number between 0 and 19 for the number of shared genres
    var sharedGenres : Double = 0.0
    for (pair <- genres1.zip(genres2)) {
      sharedGenres += pair._1 * pair._2
    }
    
    // scale the similarity according to the number of shared genres
    val scaleFactor : Double = 1 + sharedGenres/(1 + sharedGenres)
    val newScore : Double = score * scaleFactor
    
    return (moviePair, (newScore, numPairs))
  }
//SA EDIT #2A END***************************************************************************************
  
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2
    
    ((movie1, movie2), (rating1, rating2))
  }
  
  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    return movie1 < movie2
  }
  
// calculating match score using cosine similarity (default)
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  

//SA EDIT #2A START **** calculating match score using Pearson Correlation Coefficient ********************
  
  def computePearsonCoef(ratingPairs:RatingPairs): (Double, Int) = {
    
//to get the averages of movie scores, first need to get the sum of scores and get the number of scores    
    var numPairs:Int = 0
    var sum_x:Double = 0.0
    var sum_y:Double = 0.0

    for (x <- ratingPairs) 
    {
      sum_x += x._1
      sum_y += x._2
      numPairs += 1
    }     

    var x_mean = sum_x/numPairs
    var y_mean = sum_x/numPairs
    
    var numerator:Double = 0.0
    var xdiffsq:Double = 0.0
    var ydiffsq:Double = 0.0
    
    for (y <- ratingPairs) 
    {
      numerator += (y._1 - x_mean ) * (y._2 - y_mean)
      xdiffsq += (y._1 - x_mean)*(y._1 - x_mean)
      ydiffsq += (y._2 - y_mean)*(y._2 - y_mean)

    }
    
    var denominator:Double = 0.0
    var score:Double = 0.0
    
    denominator = sqrt(xdiffsq*ydiffsq)
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }

//SA EDIT #2A END***************************************************************************************
    
//SA EDIT #2B START **** calculating match score using Pearson Correlation Coefficient (HRB) ***********
  
  def computePearsonCorrelation(ratingPairs:RatingPairs): (Double, Int) = {
    
    var x_bar:Double = 0.0
    var y_bar:Double = 0.0
    var numPairs:Int = 0
    
    for (rating <- ratingPairs) {
      x_bar += rating._1
      y_bar += rating._2
      numPairs += 1
    }
    x_bar = x_bar / numPairs
    y_bar = y_bar / numPairs
    
    var numerator:Double = 0.0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    
    for (rating <- ratingPairs) {
      numerator += (rating._1 - x_bar) * (rating._2 - y_bar)
      sum_xx += (rating._1 - x_bar) * (rating._1 - x_bar)
      sum_yy += (rating._2 - y_bar) * (rating._2 - y_bar)
    }
    
    val score:Double = numerator / (sqrt(sum_xx) * sqrt(sum_yy))
    return (score, numPairs)
  }

//SA EDIT #2B END***************************************************************************************  

  
 
 /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    
    println("\nLoading movie names...")
    val nameDict = loadMovieNamesGenres()
    
    val data = sc.textFile("../ml-100k/u.data")

//SA EDIT #1 START *********** adding rating score filter early on ***********************************
    
// Map ratings to key / value pairs: user ID => movie ID, rating
val ratings = data.map(l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))   
      
// filter the RDD to only include movies rated above a specified threshold (first argument of the command line)
    val ratingsFiltered = ratings.filter(l => l._2._1 > args(1).toInt)
    
//SA EDIT #1 END*************************************************************************************** 
    
    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    val joinedRatings = ratingsFiltered.join(ratingsFiltered)   
    
    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs.
    val moviePairs = uniqueJoinedRatings.map(makePairs)

    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
    val moviePairRatings = moviePairs.groupByKey()

    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.
    val moviePairSimilarities = moviePairRatings.mapValues(computePearsonCoef)
 
     // Bring in the genres as Array[Int]s
    val moviePairsGenres = moviePairSimilarities.map(x => (x._1, x._2, (nameDict(x._1._1)._2, nameDict(x._1._2)._2)))
    
    // Calculate the number of shared genres and weight the score accordingly
    val moviePairWeightedSimilarities = moviePairsGenres.map(weightByGenre).cache()
    
    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")
    
    

//SA EDIT #4 START *********** use co-occurence to weight the score ***********************************
    
    // We now have (movie1, movie2) => (score, coOccurence volume)
    // Now get the sum of coOccurence  

    val total_0 = moviePairWeightedSimilarities.map(x => (x._2._2))    
    val total_1 = total_0.map(x => (x, 1))
    val total_2 = total_1.map(x => (x._2, x._1)).reduceByKey( (x,y) => x + y )
    val total_3 = total_2.map(x => (x._2))

    var totalcoOccurences:Double =  total_3.max()
    
    // Now we multiply the score by the number of occurences as a fraction of the total number of occurences
    // NOTE: we multiply the score by 10000 to scale it to something reasonable
    val weightedscores = moviePairSimilarities.map( x => ((x._1._1,x._1._2),(x._2._1*((x._2._2/totalcoOccurences)*10000), x._2._2))).cache()

//SA EDIT #4 END***************************************************************************************     

//SA EDIT #3 START *********** Extract similarities for the movie we care about that are "good" *******
    
    // Extract similarities for the movie we care about that are "good".
    if (args.length > 0) {
      val movieID:Int = args(0).toInt
  // changed scoreThreshold from 0.97 to the second argument on the command line 
      val scoreThreshold = args(2).toDouble
  // changed coOccurenceThreshold from 50.0 the third argument on the command line 
      val coOccurenceThreshold = args(3).toInt
      
//SA EDIT #3 END*************************************************************************************** 
         
      // Filter for movies with this sim that are "good" as defined by
      // our quality thresholds above     
      
      val filteredResults = weightedscores.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
      
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        // Display the similarity result that isn't the movie we're looking at
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tmatch score: " + sim._1 + "\tCo-occurence: " + sim._2)
      }
    }
  }
}