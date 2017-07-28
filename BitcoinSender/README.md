Block.io Java
============

Java API wrapper for http://block.io

This is a Java API wrapper for communication with the block.io wallet service. It is designed to completely abstract the API into Java objects that give you access to all data you'll need to access your wallets at block.io.  

The client is fully synchronous in its current implementation. If you need it to be asynchronous, you have to implement that according to your needs.  

Status
---
This is still work in progress, but "real world" tests look good so far. Unit tests mostly fail, I think due to wrong test vectors. The actual withdrawal works fine though. Help welcome :smile:

Build
----
1. Make sure you have at least JDK 1.6 and a current version of Maven  
2. Clone this repository
3. Run `mvn clean install` (Tests are currently disabled by default due to them breaking)

Usage
---
Include the library in your `pom.xml`:
```
<dependency>
  <groupId>io.block.api</groupId>
  <artifactId>blockio</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```  
See [SampleClient.java](src/main/java/io/block/api/SampleClient.java) for a few examples of usage. See below for docs on what you can do.

Documentation
---
You can find the JavaDocs [right here](https://langerhans.github.io/blockio-java).
