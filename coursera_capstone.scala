// ********************************************************************************
// * SETUP *************************************************************
// ********************************************************************************

// Setup
val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
import sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.matching.Regex

val data_seed = 0
val data_path = "../../detroit_data/"

// Cleanup function to find_number
val num_regex = "[0-9]+([,.][0-9]+)?".r
val find_number = udf((data:String) => data match {
  case null => None
  case data => num_regex.findFirstIn(data).map(_.toDouble)
})

// Cleanup function to get the coordinates
val coords_regex = "\\(.*?\\)".r
val get_coordinate = (data:String, index:Integer) => data match {
case null => None
case data => coords_regex
  .findFirstIn(data)
  .map( _.replaceAll("[\\(\\)]", "").split(",")(index).trim.toDouble)
}
val get_latitude  = udf((data:String) => get_coordinate(data, 0))
val get_longitude = udf((data:String) => get_coordinate(data, 1))


// Discretization functions
import scala.math.BigDecimal.RoundingMode
def discretizeCoord(coord:Double, interval:BigDecimal, offset:BigDecimal = BigDecimal("0")) : BigDecimal = {
  val c = BigDecimal(coord.toString) 
  val lower_point = (((c + offset)/interval).toInt * interval) - offset
  val center = lower_point + (interval/2)
  return center
}

def bestGrid(lat: Double, lng: Double, interval:BigDecimal) : (BigDecimal, BigDecimal) = {
  val offset = interval / 2
  // Grid 0 no overlap
  val g0_lat = discretizeCoord(lat, interval)
  val g0_lng = discretizeCoord(lng, interval)
  // Grid 1 half overlap
  val g1_lat = discretizeCoord(lat, interval, offset)
  val g1_lng = discretizeCoord(lng, interval, offset)
  // Compute manhattan distance
  val g0_dist = (g0_lat - lat).abs + (g0_lng - lng).abs
  val g1_dist = (g1_lat - lat).abs + (g1_lng - lng).abs
  // Return
  if (g0_dist <= g1_dist)
    return (g0_lat, g0_lng)
  else
    return (g1_lat, g1_lng)
}
val bestGridStr = udf((x:Double, y:Double) => Seq(bestGrid(x, y, BigDecimal("0.0001"))).mkString("[", ",", "]"))
val bestGridLat = udf((x:Double, y:Double) => bestGrid(x, y, BigDecimal("0.0001"))._1)
val bestGridLng = udf((x:Double, y:Double) => bestGrid(x, y, BigDecimal("0.0001"))._2)

// Labeling functions
val labeludf = udf((c: Long) => if (c >= 1) 1.0 else 0.0)

// Lets define this class to represent geo localized data later on
case class GeoData(lat:Double, lng:Double, value:Double, group:String = "blue")


// ********************************************************************************
// * BLIGHT VIOLATION *************************************************************
// ********************************************************************************

// --------------------------------------------------------------------------------
// Load and cleanup
// --------------------------------------------------------------------------------

var blight_violations = sqlContext
  // Read the csv
  .read.format("com.databricks.spark.csv")
  .option("header",      "true")
  .option("inferSchema", "true")      // Automatic schema inference
  .option("parserLib",   "UNIVOCITY") // This configuration solves the multi-line field issue.
  .load(data_path + "detroit-blight-violations.csv")
  .cache()  // There is a weird bug with spark and If I want to cache new columns later I need to cache here too
  // Parse numerical columns
  .withColumn("JudgmentAmt", find_number($"JudgmentAmt"))
  .withColumn("FineAmt",     find_number($"FineAmt"))
  .withColumn("AdminFee",    find_number($"AdminFee"))
  .withColumn("LateFee",     find_number($"LateFee"))
  .withColumn("StateFee",    find_number($"StateFee"))
  .withColumn("CleanUpCost", find_number($"CleanUpCost"))
  // Extract the geo localization data
  .filter("ViolationAddress LIKE '%(%,%)%'") // Only 3 rows without geodata will be discarded
  .withColumn("Violation_lat", get_latitude($"ViolationAddress"))
  .withColumn("Violation_lng", get_longitude($"ViolationAddress"))
  .withColumn("Mailing_lat",   get_latitude($"MailingAddress"))
  .withColumn("Mailing_lng",   get_longitude($"MailingAddress"))


var blight_violations_detroit = blight_violations
  // Filter detroit bounds approx.
  .filter("Violation_lat between 42.257 and 42.453")
  .filter("Violation_lng between -83.308 and -82.896")
  
// --------------------------------------------------------------------------------
// Schema and data exploration
// --------------------------------------------------------------------------------

blight_violations.printSchema()
blight_violations.count()
widgets.TableChart(blight_violations.take(5))

// --------------------------------------------------------------------------------
// Pie Charts
// --------------------------------------------------------------------------------


widgets.PieChart(blight_violations.groupBy("MailingState").count().collect())
widgets.PieChart(blight_violations.groupBy("MailingCity").count().collect())
widgets.PieChart(blight_violations.groupBy("MailingZipCode").count().collect())
widgets.PieChart(blight_violations
  .select("ViolationCode")
  .na.fill(Map("ViolationCode" -> "unknown"))
  .groupBy("ViolationCode")
  .count()
  .collect()
)

// --------------------------------------------------------------------------------
// Average fee by street number
// --------------------------------------------------------------------------------

widgets.BarChart(blight_violations
  .groupBy("ViolationStreetNumber")
  .agg(avg("JudgmentAmt"), count("JudgmentAmt"))
  .filter("count(JudgmentAmt) > 5") // Lets ask for at least 5 ocurrences in that street to make the average
  .drop("count(JudgmentAmt)")
  .sort(desc("avg(JudgmentAmt)"))
  .take(200)
)

// --------------------------------------------------------------------------------
// Geo map
// --------------------------------------------------------------------------------

val blight_points = blight_violations_detroit
  .select("Violation_lat", "Violation_lng", "FineAmt")
  .sample(false, 0.008, data_seed)
  .collect()
  .map( r => 
    GeoData(r.getAs[Double]("Violation_lat"), 
            r.getAs[Double]("Violation_lng"), 
            scala.math.sqrt(r.getAs[Double]("FineAmt") / 3.1416) / 3)
  )
widgets.GeoPointsChart(blight_points,
                       latLonFields=Some(("lat", "lng")), 
                       rField=Some("value"), 
                       colorField=Some("group"),
                       maxPoints=2000,
                       sizes=(800, 400))


// ********************************************************************************
// * DEMOLITION *******************************************************************
// ********************************************************************************

var demolition_permits = sqlContext.read
  .format("com.databricks.spark.csv")
  // For some reason univocity doesn't work well with tsv
  // I've had to replace \n characters for \b 
  // because the default parser doesn't allow new lines even inside quotes
  //.option("parserLib", "UNIVOCITY") // This configuration solves the multi-line field issue.
  .option("delimiter", "\t")        // This one is a .tsv
  .option("header", "true")
  .option("inferSchema", "true")    // Automatic schema inference
  .load(data_path + "detroit-demolition-permits.tsv")
  .cache()
  // Extract the geo localization data
  .filter("site_location LIKE '%(%,%)%'") // Only 3 rows without geodata will be discarded
  .withColumn("site_lat", get_latitude($"site_location"))
  .withColumn("site_lng", get_longitude($"site_location"))
  // Filter detroit bounds approx.
  .filter("site_lat between 42.257 and 42.453")
  .filter("site_lng between -83.308 and -82.896")


demolition_permits.printSchema()
demolition_permits.count()
demolition_permits.take(5)


// --------------------------------------------------------------------------------
// Geo map
// --------------------------------------------------------------------------------

val demolition_points = demolition_permits
  .select("site_lat", "site_lng")
  //.sample(false, 0.1, data_seed)
  .collect()
  .map( r => 
    GeoData(r.getAs[Double]("site_lat"), 
            r.getAs[Double]("site_lng"), 
            7,
           "black")
  )

widgets.GeoPointsChart(demolition_points,
                       latLonFields=Some(("lat", "lng")), 
                       rField=Some("value"), 
                       colorField=Some("group"),
                       maxPoints=1000,
                       sizes=(800, 400))

// ********************************************************************************
// * 311 CALLS ********************************************************************
// ********************************************************************************

// --------------------------------------------------------------------------------
// Load and cleanup
// --------------------------------------------------------------------------------

var calls_311 = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")    // Automatic schema inference
  .option("parserLib", "UNIVOCITY") // This configuration solves the multi-line field issue.
  .load(data_path + "detroit-311.csv")
  .cache()
  // Parse number formats
  .withColumn("lat", $"lat".cast(DoubleType))
  .withColumn("lng", $"lng".cast(DoubleType))
  .withColumn("rating", $"rating".cast(IntegerType))
  // Delete rows with null lat, lng or rating
  .na.drop("all", "lat" :: "lng" :: "rating" :: Nil)
  // Filter detroit bounds approx.
  .filter("lat between 42.257 and 42.453")
  .filter("lng between -83.308 and -82.896")

calls_311.printSchema()
calls_311.count()

// --------------------------------------------------------------------------------
// Bar chart by call rating
// --------------------------------------------------------------------------------

widgets.BarChart(calls_311.na.drop("all", "rating" :: Nil).groupBy("rating").count().collect())

// --------------------------------------------------------------------------------
// Geo map
// --------------------------------------------------------------------------------

val call_points = calls_311
  .select("lat", "lng", "rating")
  .sample(false, 0.05, data_seed)
  .collect()
  .map( r => 
    GeoData(r.getAs[Double]("lat"), 
            r.getAs[Double]("lng"), 
            r.getAs[Int]("rating").toDouble * 2.5,
           "white")
  )

widgets.GeoPointsChart(call_points,
                       latLonFields=Some(("lat", "lng")), 
                       rField=Some("value"), 
                       colorField=Some("group"),
                       maxPoints=1000,
                       sizes=(800, 400))

// ********************************************************************************
// * CRIMES ***********************************************************************
// ********************************************************************************

// --------------------------------------------------------------------------------
// Load and cleanup
// --------------------------------------------------------------------------------

var crimes = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")    // Automatic schema inference
  .option("parserLib", "UNIVOCITY") // This configuration solves the multi-line field issue.
  .load(data_path + "detroit-crime.csv")
  .cache()
  // Filter detroit bounds approx.
  .filter("LAT between 42.257 and 42.453")
  .filter("LON between -83.308 and -82.896")

crimes.printSchema()
crimes.count()
crimes.take(5)

// --------------------------------------------------------------------------------
// Geo map
// --------------------------------------------------------------------------------

val crime_points = crimes
  .select("LAT", "LON")
  .na.drop("all", "LAT" :: "LON" :: Nil)
  .filter("LAT between -360 and 360")
  .filter("LON between -360 and 360")
  .sample(false, 0.02, data_seed)
  .collect()
  .map( r => 
    GeoData(r.getAs[Double]("LAT"), 
            r.getAs[Double]("LON"),
            10.0,
           "red")
  )
widgets.GeoPointsChart(crime_points,
                       latLonFields=Some(("lat", "lng")), 
                       rField=Some("value"), 
                       colorField=Some("group"),
                       maxPoints=1000,
                       sizes=(800, 400))


// ********************************************************************************
// * DISCRETIZATION ***************************************************************
// ********************************************************************************

// Blight discretization
blight_violations_detroit = blight_violations_detroit
  .withColumn("building", bestGridStr($"Violation_lat", $"Violation_lng"))
  .withColumn("building_lat", bestGridLat($"Violation_lat", $"Violation_lng"))
  .withColumn("building_lng", bestGridLng($"Violation_lat", $"Violation_lng"))
  
// Demolition discretization
demolition_permits = demolition_permits
  .withColumn("building", bestGridStr($"site_lat", $"site_lng"))
  .withColumn("building_lat", bestGridLat($"site_lat", $"site_lng"))
  .withColumn("building_lng", bestGridLng($"site_lat", $"site_lng"))

// 311 Calls discretization
calls_311 = calls_311
  .withColumn("building", bestGridStr($"lat", $"lng"))
  .withColumn("building_lat", bestGridLat($"lat", $"lng"))
  .withColumn("building_lng", bestGridLng($"lat", $"lng"))

// Crime discretization
crimes = crimes
  .withColumn("building", bestGridStr($"LAT", $"LON"))
  .withColumn("building_lat", bestGridLat($"LAT", $"LON"))
  .withColumn("building_lng", bestGridLng($"LAT", $"LON"))
  
// --------------------------------------------------------------------------------
// Bar chart
// --------------------------------------------------------------------------------
  
widgets.BarChart(blight_violations_detroit.groupBy("building").count().collect())
 
					   
// ********************************************************************************
// * TRAINING DATA PREPARATION ****************************************************
// ********************************************************************************			   
					   
// List of distinct buildings to be marked as "blighted"
var demolition_data = demolition_permits
  .groupBy("building", "building_lat", "building_lng")
  .count()
  .withColumnRenamed("count", "demolition_count")
  .cache()

// 311 Calls data by building
var calls_data = calls_311
  .groupBy("building", "building_lat", "building_lng")
  .agg(count("building"), avg("rating"), min("rating"), max("rating"))
  .withColumnRenamed("count(building)", "calls_count")
  .withColumnRenamed("avg(rating)", "calls_avg_rating")
  .withColumnRenamed("min(rating)", "calls_min_rating")
  .withColumnRenamed("max(rating)", "calls_max_rating")
  .cache()

// Crimes data by building
val crimes_data = crimes
  .groupBy("building", "building_lat", "building_lng")
  .count()
  .withColumnRenamed("count", "crimes_count")
  .cache()


// Blight data by building
var blight_data = blight_violations_detroit
  .groupBy("building", "building_lat", "building_lng")
  .agg(count("building"), sum("FineAmt"), avg("FineAmt"), min("FineAmt"), max("FineAmt"))
  .withColumnRenamed("count(building)", "blights_count")
  .withColumnRenamed("sum(FineAmt)", "blights_sum_FineAmt")
  .withColumnRenamed("avg(FineAmt)", "blights_avg_FineAmt")
  .withColumnRenamed("min(FineAmt)", "blights_min_FineAmt")
  .withColumnRenamed("max(FineAmt)", "blights_max_FineAmt")
  .cache()

// --------------------------------------------------------------------------------
// Blight data joined
// --------------------------------------------------------------------------------

val buildingColumns = Array("building", "building_lat", "building_lng")
var blight_data_labeled = demolition_data
   // outer joins, we want every single building to count
  .join(blight_data, buildingColumns, "outer")  // Join with labels (from demolitions)
  .join(calls_data, buildingColumns, "left_outer") // Join with 311 calls
  .join(crimes_data, buildingColumns, "left_outer") // Join with crimes
  // Null demolitions fill
  .na.fill(0, "demolition_count" :: Nil)
  // Null blights fill
  .na.fill(0, "blights_count" :: Nil)
  .na.fill(0.0, "blights_sum_FineAmt" :: "blights_avg_FineAmt" :: "blights_min_FineAmt" :: "blights_max_FineAmt" :: Nil)
  // Null calls fill
  .na.fill(0, "calls_count" :: "calls_min_rating" :: "calls_max_rating" :: Nil)
  .na.fill(0.0, "calls_avg_rating" :: Nil)
  // Null crimes fill
  .na.fill(0, "crimes_count":: Nil)
  // Rename and format the label  
  .withColumnRenamed("demolition_count", "label")
  .withColumn("label", labeludf($"label"))
  .cache()

blight_data_labeled
					   
// ********************************************************************************
// * TRAINING *********************************************************************
// ********************************************************************************

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vectors

val featureColumns = Array(
  "building_lat", "building_lng", // Building coords
  "blights_count", "blights_sum_FineAmt", "blights_avg_FineAmt", "blights_min_FineAmt", "blights_max_FineAmt", // Blight features
  "calls_count", "calls_avg_rating", "calls_min_rating", "calls_max_rating", // Calls features
  "crimes_count") // Crime features

// Prepare out training dataset
val assembler = new VectorAssembler()
  .setInputCols(featureColumns)
  .setOutputCol("features")

val dataset = assembler
  .transform(blight_data_labeled)
  .withColumnRenamed("building", "id")
  .select("id", "features", "label")
  .withColumn("label", $"label".cast(DoubleType))
  .na.drop("all", "label" :: Nil ) // everything must be labelled

val split = dataset.randomSplit(Array(0.8, 0.2), seed = data_seed)
val training_data = split(0)
val test_data = split(1)

dataset.groupBy("label").count().collect()

// --------------------------------------------------------------------------------
//  Logistic Regression
// --------------------------------------------------------------------------------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

// Setup our ML pipeline 
val lr = new LogisticRegression()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setMaxIter(10)
val pipeline = new Pipeline().setStages(Array(lr))

val evaluator = new BinaryClassificationEvaluator

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(new ParamGridBuilder().build()) // No parameter search
  .setNumFolds(5)
  
// Run cross-validation, and fit the estimator
val cvModel = cv.fit(training_data)


val predictions = cvModel.transform(test_data)
evaluator.evaluate(predictions)


import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SQLContext}

// score model on test data
val predictionsAndLabels = predictions.select("prediction", "label").map {case Row(p: Double, l: Double) => (p, l)}
// compute confusion matrix
val metrics = new MulticlassMetrics(predictionsAndLabels)
println(metrics.confusionMatrix)


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(blight_data_labeled)

// Train a RandomForest model.
val rf = new RandomForestClassifier()
  .setLabelCol(labelIndexer.getOutputCol)
  .setFeaturesCol("features")
  .setNumTrees(10)

val rf_pipeline = new Pipeline().setStages(Array(labelIndexer, rf))

val rf_evaluator = new BinaryClassificationEvaluator

val rf_cv = new CrossValidator()
  .setEstimator(rf_pipeline)
  .setEvaluator(rf_evaluator)
  .setEstimatorParamMaps(new ParamGridBuilder().build()) // No parameter search
  .setNumFolds(5)

// Run cross-validation, and fit the estimator
val rfModel = rf_cv.fit(training_data)

val rf_predictions = rfModel.transform(test_data)
rf_evaluator.evaluate(rf_predictions)

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SQLContext}

// score model on test data
val rf_predictionsAndLabels = rf_predictions.select("prediction", "label").map {case Row(p: Double, l: Double) => (p, l)}
// compute confusion matrix
val rf_metrics = new MulticlassMetrics(rf_predictionsAndLabels)
println(rf_metrics.confusionMatrix)