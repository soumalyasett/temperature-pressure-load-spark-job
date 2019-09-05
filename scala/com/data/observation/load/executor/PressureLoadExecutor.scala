/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the pressure observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1861),(1862-1937),
 (1938-1960),(1961-2012),(2013-2017,manual station),(2013-2017,automatic station)}----*/
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

class PressureLoadExecutor(var spark: SparkSession) {
  val logger = Logger.getLogger(this.getClass.getName)
  
  /*-----------------Below snippet is written to pass the 4 values for each data set from the temperaturePressure.properties file
1.link for the different data set
2.final temporary table name from which data is loaded to final table
3.create statement of the final table
4.overwrite query for the final table(fields are separated by \n)
--------------------*/

  val cfg = ConfigFactory.parseFile(new File("C:/cfg_files/temperaturePressure.properties"))
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

  /*--------------------------------------------------------------------------------------------------*/

  val dataCleansingActivity = new DataCleansingActivity(spark)
  val schemasForDfCreation = new SchemasForDfCreation
  
   def processPressureDataset1 {
    logger.info("creating dataframe FOR THE RANGE FOR THE RANGE 1756 TO 1858")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd1,schemasForDfCreation.pressureSchema1)
         .coalesce(1)
         .createOrReplaceTempView(metric6(1))
    logger.info("creating final table FOR THE RANGE 1756 TO 1858")
    spark.sql(metric6(2))
    logger.info("inserting data into final table FOR THE RANGE 1756 TO 1858")
    spark.sql(metric6(3))  
  
}
  
  def processPressureDataset2 {
    logger.info("creating dataframe FOR THE RANGE 1859 TO 1861")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd2,schemasForDfCreation.pressureSchema2)
         .coalesce(1)
         .createOrReplaceTempView(metric7(1))
    logger.info("creating final table FOR THE RANGE 1859 TO 1861")
    spark.sql(metric7(2))
    logger.info("inserting data into final table FOR THE RANGE 1859 TO 1861")
    spark.sql(metric7(3))  
  
}
  
  def processPressureDataset3 {
    logger.info("creating dataframe FOR THE RANGE 1862 TO 1937")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd3,schemasForDfCreation.pressureSchema3)
         .coalesce(1)
         .createOrReplaceTempView(metric8(1))
    logger.info("creating final table FOR THE RANGE 1862 TO 1937")
    spark.sql(metric8(2))
    logger.info("inserting data into final table FOR THE RANGE 1862 TO 1937")
    spark.sql(metric8(3))  
  
}
  
  def processPressureDataset4 {
    logger.info("creating dataframe FOR THE RANGE 1938 TO 1960")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd4,schemasForDfCreation.pressureSchema4)
         .coalesce(1)
         .createOrReplaceTempView(metric9(1))
    logger.info("creating final table FOR THE RANGE 1938 TO 1960")
    spark.sql(metric9(2))
    logger.info("inserting into final table FOR THE RANGE 1938 TO 1960")
    spark.sql(metric9(3))  
  
}
  
  def processPressureDataset5 {
    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd5,schemasForDfCreation.pressureSchema5)
         .coalesce(1)
         .createOrReplaceTempView(metric10(1))
    logger.info("creating final table FOR THE RANGE 1961 TO 2012")
    spark.sql(metric10(2))
    logger.info("inserting into final table FOR THE RANGE 1961 TO 2012")
    spark.sql(metric10(3))  
  
}
  
  def processPressureDataset6 {
    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd6,schemasForDfCreation.pressureSchema6)
         .coalesce(1)
         .createOrReplaceTempView(metric11(1))
    logger.info("creating final table FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.sql(metric11(2))
    logger.info("inserting into final table FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")
    spark.sql(metric11(3))  
  
}
  
  def processPressureDataset7 {
    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.createDataFrame(dataCleansingActivity.finalPressureDatasetRdd7,schemasForDfCreation.pressureSchema7)
         .coalesce(1)
         .createOrReplaceTempView(metric12(1))
    logger.info("creating final table FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.sql(metric12(2))
    logger.info("inserting into final table FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")
    spark.sql(metric12(3))  
  
}
}