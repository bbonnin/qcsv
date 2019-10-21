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


TODO

## TODO

- [x] ignore header
- [ ] consider all columns as varchar
- [ ] use it as a lib (provide an API, QueryRunner is enough ? Add example)
- [x] partial types (set the type only for a few columns)
- [x] add process time
- [x] more logs when inserting (nb of inserts, time)
- [x] use a library for parsing the csv
- [ ] locale for numbers 
- [ ] if header present use the name in the header instead of c1, c2, ...
- [ ] config in a property file