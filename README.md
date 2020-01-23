# Anodot stage library for Streamsets Data Collector

### How to build
1. Clone [Streamsets Data Collector repo](https://github.com/streamsets/datacollector)
2. Build required streamsets dependencies
```
mvn -pl httpcommonlib -am clean install -DskipTests
```
3. Build project
```
 mvn install package -DskipTests
```

Copy files from tarball to User Libs directory in streamsets
