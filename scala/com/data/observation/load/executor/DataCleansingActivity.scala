    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

package com.data.observation.load.executor

import org.apache.spark.sql.SparkSession
import com.typesafe.config._
import java.io.File
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.Logger

class DataCleansingActivity(var spark: SparkSession) {
  
  val logger = Logger.getLogger(this.getClass.getName)
  
   val cfg = ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties"))
  var metric1 = ArrayBuffer[String]()
  metric1.appendAll(cfg.getString("metric1").split("\n"))
  var metric2 = ArrayBuffer[String]()
  metric2.appendAll(cfg.getString("metric2").split("\n"))
  var metric3 = ArrayBuffer[String]()
  metric3.appendAll(cfg.getString("metric3").split("\n"))
  var metric4 = ArrayBuffer[String]()
  metric4.appendAll(cfg.getString("metric4").split("\n"))
  var metric5 = ArrayBuffer[String]()
  metric5.appendAll(cfg.getString("metric5").split("\n"))
  var metric6 = ArrayBuffer[String]()
  metric6.appendAll(cfg.getString("metric6").split("\n"))
  var metric7 = ArrayBuffer[String]()
  metric7.appendAll(cfg.getString("metric7").split("\n"))
  var metric8 = ArrayBuffer[String]()
  metric8.appendAll(cfg.getString("metric8").split("\n"))
  var metric9 = ArrayBuffer[String]()
  metric9.appendAll(cfg.getString("metric9").split("\n"))
  var metric10 = ArrayBuffer[String]()
  metric10.appendAll(cfg.getString("metric10").split("\n"))
  var metric11 = ArrayBuffer[String]()
  metric11.appendAll(cfg.getString("metric11").split("\n"))
  var metric12 = ArrayBuffer[String]()
  metric12.appendAll(cfg.getString("metric12").split("\n"))
  
  def Dataset_load(i: String): scala.io.BufferedSource = {
    val temperatureDataset = scala.io.Source.fromURL(i)
      temperatureDataset
    }
  
  def finalTemperatureDatasetRdd1 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    
    val temperatureDataset1 = Dataset_load(metric1(0))
    val temperatureDatasetRdd1 = spark.sparkContext.parallelize(temperatureDataset1.mkString.split("\n"))
    val finalTemperatureDatasetRdd1 = temperatureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6))) //0th column not taken because it is blank
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalTemperatureDatasetRdd1
  }
  
  def finalTemperatureDatasetRdd2 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
  
  val temperatureDataset2 = Dataset_load(metric2(0))
  val temperatureDatasetRdd2 = spark.sparkContext.parallelize(temperatureDataset2.mkString.split("\n"))
  val finalTemperatureDatasetRdd2 = temperatureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8)
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble))
      finalTemperatureDatasetRdd2
  }
  
  def finalTemperatureDatasetRdd3 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
 
    val temperatureDataset3 = Dataset_load(metric3(0))
    val temperatureDatasetRdd3 = spark.sparkContext.parallelize(temperatureDataset3.mkString.split("\n"))
    val finalTemperatureDatasetRdd3 = temperatureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))
      finalTemperatureDatasetRdd3
  }
  
  def finalTemperatureDatasetRdd4 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    
    val temperatureDataset4 = Dataset_load(metric4(0))
    val temperatureDatasetRdd4 = spark.sparkContext.parallelize(temperatureDataset4.mkString.split("\n"))
    val finalTemperatureDatasetRdd4 = temperatureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))
      finalTemperatureDatasetRdd4
    
  }
  
  def finalTemperatureDatasetRdd5 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    
    val temperatureDataset5 = Dataset_load(metric5(0))
    val temperatureDatasetRdd5 = spark.sparkContext.parallelize(temperatureDataset5.mkString.split("\n"))
    val finalTemperatureDatasetRdd5 = temperatureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))
      finalTemperatureDatasetRdd5
  }
  
  def finalPressureDatasetRdd1 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
   
    val pressureDataset1 = Dataset_load(metric6(0))
    val pressureDatasetRdd1 = spark.sparkContext.parallelize(pressureDataset1.mkString.split("\n"))
    val finalPressureDatasetRdd1 = pressureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))
      finalPressureDatasetRdd1
  }
  
  def finalPressureDatasetRdd2 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
   
    val pressureDataset2 = Dataset_load(metric7(0))
    val pressureDatasetRdd2 = spark.sparkContext.parallelize(pressureDataset2.mkString.split("\n"))
    val finalPressureDatasetRdd2 = pressureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8), r.split(",")(9), r.split(",")(10), r.split(",")(11)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble, a(9).toDouble, a(10).toDouble, a(11).toDouble))
      finalPressureDatasetRdd2
  }
  
  def finalPressureDatasetRdd3 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    
    val pressureDataset3 = Dataset_load(metric8(0))
    val pressureDatasetRdd3 = spark.sparkContext.parallelize(pressureDataset3.mkString.split("\n"))
    val finalPressureDatasetRdd3 = pressureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalPressureDatasetRdd3
 }
  
  def finalPressureDatasetRdd4 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
  
    val pressureDataset4 = Dataset_load(metric9(0))
    val pressureDatasetRdd4 = spark.sparkContext.parallelize(pressureDataset4.mkString.split("\n"))
    val finalPressureDatasetRdd4 = pressureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalPressureDatasetRdd4
  }
  
  def finalPressureDatasetRdd5 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
  
    val pressureDataset5 = Dataset_load(metric10(0))
    val pressureDatasetRdd5 = spark.sparkContext.parallelize(pressureDataset5.mkString.split("\n"))
    val finalPressureDatasetRdd5 = pressureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalPressureDatasetRdd5
  }
  
  def finalPressureDatasetRdd6 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
   
    val pressureDataset6 = Dataset_load(metric11(0))
    val pressureDatasetRdd6 = spark.sparkContext.parallelize(pressureDataset6.mkString.split("\n"))
    val finalPressureDatasetRdd6 = pressureDatasetRdd6.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalPressureDatasetRdd6
}
  
  def finalPressureDatasetRdd7 : org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = {
    
    val pressureDataset7 = Dataset_load(metric12(0))
    val pressureDatasetRdd7 = spark.sparkContext.parallelize(pressureDataset7.mkString.split("\n"))
    val finalPressureDatasetRdd7 = pressureDatasetRdd7.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))
      finalPressureDatasetRdd7
}
  
}