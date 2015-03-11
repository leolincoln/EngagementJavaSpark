#~/spark/bin/spark-submit --class "HitchCockProcess" --master "spark://wolf.iems.northwestern.edu:7077" target/TestCassandraMaven-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 /opt/cloudera/parcels/spark-1.2.0-bin-cdh4/bin/spark-submit --class "HcProcess" --master "spark://wolf.iems.northwestern.edu:7077" TestCassandraMaven-0.0.1-SNAPSHOT-jar-with-dependencies.jar |tee runTimeCmd.txt
 
