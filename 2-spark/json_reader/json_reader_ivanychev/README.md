To run:

```bash
~/Downloads/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
  --master "local[*]" \
  --driver-java-options "-Dlog4j.configuration=file:/Users/ivanychev/Code/de/2-spark/json_reader/json_reader_ivanychev/src/main/resources/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/Users/ivanychev/Code/de/2-spark/json_reader/json_reader_ivanychev/src/main/resources/log4j.properties" \
  --class com.github.mrpowers.my.cool.project.JsonReader \
  /Users/ivanychev/Code/de/2-spark/json_reader/json_reader_ivanychev/target/scala-2.11/json_reader_ivanychev-assembly-0.0.1.jar "/Users/ivanychev/Downloads/winemag-data-130k-v2.json"
```