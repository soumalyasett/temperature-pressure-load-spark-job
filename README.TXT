This file describes the process to be followed for loading the temperature and pressure observations into hive tables.
Steps:

1. Build the jar and copy it to any path of the edge node of the cluster
2. Also copy the hive-site.xml in the same path, this will be needed for accessing the hive database
3. create the log4j.properties and loadTrigger.sh file in the same path
4. Also copy the temperaturePressure.properties file(file details can be referred from the comments in the file)
5. Then run the shell script(sh loadTrigger.sh). This will do the spark submit in spark version 2 and invoke the spark job.Default memory configuration will be picked up.
6. After the completion of the job, a mail will be triggered based upon success or failure .(edit the mail id accordingly)

Also unit testing is done to validate the count at source and final table.(temperaturePressureProcessorTest.scala)

Assumptions :

1. Code has been run and tested in local machine(ide : eclipse , build tool : maven)(necessary changes to be done for running in a cluster , example while creating spark session)
2. camel case used 
3. For loading temperature datasets , 5 functional modules developed .For loading pressure datasets , 7 functional modules developed. (different functions have been developed so that it won't be compulsory to run all the jobs at a time, we can comment according to requirement)
5. Unit testing done using Junit.
