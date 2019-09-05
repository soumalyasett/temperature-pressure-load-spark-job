/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the temperature observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1960),(1961-2012),
 (2013-2017, manual station),(2013-2017, automatic station)}----*/
/*---------------------------------------------------------------------------------------------*/

package com.data.observation.load.executor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.log4j.Logger
import com.typesafe.config._
import java.io.File
import scala.collection.mutable.ArrayBuffer

class TemperatureLoadExecutor(var spark: SparkSession) {
  val logger = Logger.getLogger(this.getClass.getName)

  /*-----------------Below snippet is written to pass the 4 values for each data set from the temperaturePressure.properties file
1.link for the different data set
2.final temporary table name from which data is loaded to final table
3.create statement of the final table
4.overwrite query for the final table(fields are separated by \n)
--------------------*/

  val cfg = ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties")) //pass this path as argument
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

  /*--------------------------------------------------------------------------------------------------*/

  val dataCleansingActivity = new DataCleansingActivity(spark) //for getting refined rdd
  val schemasForDfCreation = new SchemasForDfCreation //for getting the schema
  
  def processTemperatureDataset1 {
    logger.info("creating dataframe FOR THE RANGE 1756 TO 1858")
    spark.createDataFrame(dataCleansingActivity.finalTemperatureDatasetRdd1,schemasForDfCreation.temperatureSchema1)
         .coalesce(1)
         .createOrReplaceTempView(metric1(1))
    logger.info("creating final table FOR THE RANGE 1756 TO 1858")
    spark.sql(metric1(2))
    logger.info("inserting data into final table FOR THE RANGE 1756 TO 1858")
    spark.sql(metric1(3))   
  }
  
  def processTemperatureDataset2 {
    logger.info("creating dataframe FOR THE RANGE 1859 TO 1960")
    spark.createDataFrame(dataCleansingActivity.finalTemperatureDatasetRdd2,schemasForDfCreation.temperatureSchema2)
         .coalesce(1)
         .createOrReplaceTempView(metric2(1))
    logger.info("creating final table FOR THE RANGE 1859 TO 1960")
    spark.sql(metric2(2))
    logger.info("inserting data into final table FOR THE RANGE 1859 TO 1960")
    spark.sql(metric2(3))
  }
  
  def processTemperatureDataset3 {
    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")
    spark.createDataFrame(dataCleansingActivity.finalTemperatureDatasetRdd3,schemasForDfCreation.temperatureSchema3)
         .coalesce(1)
         .createOrReplaceTempView(metric3(1))
    logger.info("creating final table FOR THE RANGE 1961 TO 2012")
    spark.sql(metric3(2))
    logger.info("inserting data into final table FOR THE RANGE 1961 TO 2012")
    spark.sql(metric3(3))
  }
  
  def processTemperatureDataset4 {
    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.createDataFrame(dataCleansingActivity.finalTemperatureDatasetRdd4,schemasForDfCreation.temperatureSchema4)
         .coalesce(1)
         .createOrReplaceTempView(metric4(1))
    logger.info("creating final table FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.sql(metric4(2))
    logger.info("inserting data in to final table FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.sql(metric4(3))
  }
  
  def processTemperatureDataset5 {
    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.createDataFrame(dataCleansingActivity.finalTemperatureDatasetRdd5,schemasForDfCreation.temperatureSchema5)
         .coalesce(1)
         .createOrReplaceTempView(metric5(1))
    logger.info("creating final table FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.sql(metric5(2))
    logger.info("inserting data into final table FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.sql(metric5(3))
  }
  
}
