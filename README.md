# Anodot stage library for Streamsets Data Collector

### How to build
1. Clone [Streamsets Data Collector repo](https://github.com/streamsets/datacollector-oss)
2. Switch to 3.18.0 tag and build required streamsets dependencies
```
mvn -pl httpcommonlib -am clean install -DskipTests
```
3. Build project
```
 mvn install package -DskipTests
```

Copy files from tarball to User Libs directory in streamsets
