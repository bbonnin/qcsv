# qcsv
Run SQL queries on csv files

## Build

```bash
mvn clean package
```

## Run 

```bash
java -jar /path/to/jar/qcsv.jar \
  -i data.csv -d \| -l 50 -q "select * from t0 where CURRENT_TIMESTAMP > c3"
```

## Use in your code


