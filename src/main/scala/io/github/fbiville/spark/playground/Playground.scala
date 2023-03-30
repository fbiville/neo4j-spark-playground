package io.github.fbiville.spark.playground

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.utility.DockerImageName

import scala.util.Using

object Playground {

  private val adminPassword = "letmein!"

  def main(args: Array[String]): Unit = {
    Using(startContainer("5.6.0-enterprise")) {
      container => {

        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("SparkPlayground")
          .getOrCreate()


        runBasicWrite(spark, container)
        runBasicRead(spark, container)
        runFilteredRead(spark, container)
      }
    }
  }

  private def runBasicWrite(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running basic write example")

    import spark.implicits._

    val df = List(
      Tuple2("John Doe", 32),
      Tuple2("Jane Doe", 42),
    ).toDF("name", "age")

    df.write.format("org.neo4j.spark.DataSource")
      .mode(SaveMode.Append)
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .option("labels", ":Person")
      .save()
  }


  private def runBasicRead(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running basic read example")

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .option("labels", "Person")
      .load()

    df.show()
  }

  private def runFilteredRead(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running read example with pushdown filter")

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .option("labels", ":Person")
      .load()

    df.where("name = 'John Doe'").where("age = 32").show()
  }

  private def startContainer(version: String): Neo4jContainer[_] = {
    val container = new Neo4jContainer(DockerImageName.parse("neo4j:4.4"))
    container.withAdminPassword(adminPassword)
    container.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    container.start()
    container
  }
}
