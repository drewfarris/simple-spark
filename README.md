# Some Basic Spark Stuff

Run the spark job this way:

```
spark-submit target/spark-java-1.0-SNAPSHOT.jar simple \
    -i  /path/to/input -o /path/to/output
```

To run on yarn:

```
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    target/spark-java-1.0-SNAPSHOT.jar simple \
    -m yarn \
    -i  /path/to/input -o /path/to/output
```