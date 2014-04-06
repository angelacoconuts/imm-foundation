cp crawl/target/crawl-0.0.1-SNAPSHOT-jar-with-dependencies.jar crawl/crawl-0.0.1.jar
cp filter/target/filter-0.0.1-SNAPSHOT-jar-with-dependencies.jar filter/filter-0.0.1.jar
find . -type f -name "*.log*" -delete
find . -type f -name "*~" -delete
rm -rf crawl/data
mvn clean
git add .

