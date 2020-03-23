Run example:

```bash
~/Downloads/spark-2.4.5-bin-hadoop2.7/bin/spark-submit \
  --master "local[*]" \
  --class com.example.BostonCrimesMap \
  boston-crimes-assembly-0.0.1.jar \
    /Users/ivanychev/Code/de/sandbox/crimes-in-boston/crime.csv \
    /Users/ivanychev/Code/de/sandbox/crimes-in-boston/offense_codes.csv \
    /Users/ivanychev/Code/de/sandbox/crimes-in-boston/outputs
```
