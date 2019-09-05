/*---------------------------------------------------------------------------------------------*/
/*----The below code will create the spark session and invoke the functions for loading 
 the temperature and pressure observation data into different tables----*/
/*---------------------------------------------------------------------------------------------*/

package com.data.observation.load.processor

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import com.data.observation.load.executor._

object TemperaturePressureSparkJob {
val logger = Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]) {
    
      try {
        logger.info("creating spark session")

        val spark = SparkSession.builder()
          .master("local") //remove this and compile for cluster ,mention master in spark submit command
          .appName("Atmosphere and Temperature observation data load")
          .config("spark.sql.catalogImplementation", "hive") //.enableHiveSupport() for cluster
          .getOrCreate()

        val startTime = System.currentTimeMillis()
        
/*----Calling all the 5 functions to load the temperature observations for the 5 data sets into the corresponding final tables------*/
        logger.info("temperature observation load start")
        val temperatureLoadExecutor = new TemperatureLoadExecutor(spark)
        temperatureLoadExecutor.processTemperatureDataset1;temperatureLoadExecutor.processTemperatureDataset2;temperatureLoadExecutor.processTemperatureDataset3;temperatureLoadExecutor.processTemperatureDataset4;temperatureLoadExecutor.processTemperatureDataset5
        logger.info("temperature observation load ended")
/*---------------------------------------------------------------------------------------------------------------*/  
        
 /*---Calling all the 7 functions to load the pressure observations for the 7 data sets into the corresponding final tables----- */
        logger.info("pressure observation load started")
        val pressureLoadExecutor = new PressureLoadExecutor(spark)
        pressureLoadExecutor.processPressureDataset1;pressureLoadExecutor.processPressureDataset2;pressureLoadExecutor.processPressureDataset3;pressureLoadExecutor.processPressureDataset4;pressureLoadExecutor.processPressureDataset5;pressureLoadExecutor.processPressureDataset6;pressureLoadExecutor.processPressureDataset7
        logger.info("pressure observation load ended")
 /*-----------------------------------------------------------------------------------------------------------------*/       
        val endTime = System.currentTimeMillis()
       logger.info("Total Time taken for the job: " + (endTime - startTime) / (1000) + " seconds")

        spark.stop()
      } catch {
        case ex: Exception =>
          logger.info("Below is the exception occured : \n" + ex)
          throw new Exception
      }
    

  }
}