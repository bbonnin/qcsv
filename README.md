# qcsv
Run SQL queries on csv files

> Lib based on Java 1.7 due to constraints on projects using it :(
> Impacts:
> - use Joda time instead of Java 8 classes (TO BE TESTED !!!!!!!)
> - decrease version number for some libs


## Build

```bash
mvn clean package
```

## Run 

```bash
java -jar /path/to/jar/qcsv.jar \
  -i data.csv -d \| -l 50 -q "select * from csv where CURRENT_TIMESTAMP > c3"
```

## Use in your code

TODO

## TODO

- [ ] delete database after processing (close ?)
- [x] support file name in query (for old queries using q.py)
- [x] ignore header
- [x] consider all columns as varchar
- [x] use it as a lib (provide an API, QueryRunner)
  - [ ] Add example
- [x] partial types (set the type only for a few columns)
- [x] add process time
- [x] more logs when inserting (nb of inserts, time)
- [x] use a library for parsing the csv
- [ ] locale for numbers 
- [ ] if header present use the name in the header instead of c1, c2, ...
- [ ] config in a property file
- [ ] Java 8 (check libs version in pom.xml)
