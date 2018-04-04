Dependency java project info.bliki-core
How to install https://bitbucket.org/axelclk/info.bliki.wiki/wiki/Home


cd .. (tools-root)
git clone https://bitbucket.org/axelclk/info.bliki.wiki.git

cd info.bliki.wiki
git submodule init && git submodule update 
mvn package -DskipTests -Dmaven.test.skip=true
mvn install dependency:copy-dependencies -DskipTests

cd ..
cp info.bliki.wiki/bliki-core/target/*.jar WikipediaConverter/lib/.
cp info.bliki.wiki/bliki-core/target/dependency/*.jar WikipediaConverter/lib/.

cd WikipediaConverter
mvn dependency:copy-dependencies
cp target/dependency/*.jar lib/.
cp build/lib/*.jar lib/.
