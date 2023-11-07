package io.github.fbiville.spark.playground

import org.apache.spark.sql.types.{DataTypes, StructType, StructField}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.utility.DockerImageName

import scala.util.Using

object Playground {

  private val adminPassword = "letmein!"

  def main(args: Array[String]): Unit = {
    Using(startContainer("5.13.0-enterprise")) {
      container => {

        val spark = SparkSession.builder()
          .master("local[*]")
          .appName("SparkPlayground")
          .getOrCreate()

        println(s"Running Spark version ${spark.version}")
        runBasicWrite(spark, container)
        runBasicRead(spark, container)
        runFilteredRead(spark, container)
        runAggregation(spark, container)
        runSchemaByExplicitStrategy(spark, container)
        runSchemaBySampleStrategy(spark, container)
        runSchemaByStringStrategy(spark, container)
      }
    }
  }

  private def runBasicWrite(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running basic write example")

    import spark.implicits._

    val df = List(
      ("John Doe", 32),
      ("Jane Doe", 42),
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

  private def runAggregation(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running aggregation example")

    Using.Manager { use =>
      val driver = use(createDriver(container))
      val session = use(driver.session())
      val result = session.run(
        """
          | CREATE (pe:Person {id: 1, fullName: 'Person'})
          | WITH pe
          | UNWIND range(1, 10) as id
          | CREATE (pr:Product {id: id * rand(), name: 'Product ' + id, price: id})
          | CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
        """.stripMargin)
      result.consume()

      spark.read.format("org.neo4j.spark.DataSource")
        .option("url", container.getBoltUrl)
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", adminPassword)
        .option("relationship", "BOUGHT")
        .option("relationship.source.labels", "Person")
        .option("relationship.target.labels", "Product")
        .load
        .createTempView("BOUGHT")

      val df = spark.sql(
        """SELECT `source.fullName`, MAX(`target.price`) AS max, MIN(`target.price`) AS min
          |FROM BOUGHT
          |GROUP BY `source.fullName`""".stripMargin)

      df.show()
    }
  }

  private def runSchemaByExplicitStrategy(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running schema example (explicit strategy)")

    val df = (spark.read.format("org.neo4j.spark.DataSource")
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .schema(StructType(Array(StructField("id", DataTypes.StringType), StructField("name", DataTypes.StringType))))
      .option("query", "MATCH (n:Person) WITH n LIMIT 2 RETURN id(n) as id, n.name as name")
      .load())

    df.printSchema()
    df.show()
  }

  private def runSchemaBySampleStrategy(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running schema example (sample strategy)")

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .option("query", "MATCH (n:Person) WITH n LIMIT 2 RETURN id(n) as id, n.name as name")
      .load()

    df.printSchema()
    df.show()
  }

  private def runSchemaByStringStrategy(spark: SparkSession, container: Neo4jContainer[_]): Unit = {
    println("==== Running schema example (string strategy)")

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", container.getBoltUrl)
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", adminPassword)
      .option("query", "MATCH (n:Person) WITH n LIMIT 2 RETURN id(n) as id, n.name as name")
      .option("schema.strategy", "string")
      .load()

    df.printSchema()
    df.show()

    import scala.jdk.CollectionConverters._
    val result = df.collectAsList()
    for (row <- result.asScala) {
      println(s"""ID is: ${row.getString(0).toLong}, name is: ${row.getString(1)}""")
    }

  }

  private def createDriver(container: Neo4jContainer[_]): Driver = {
    GraphDatabase.driver(container.getBoltUrl, AuthTokens.basic("neo4j", container.getAdminPassword))
  }


  private def startContainer(version: String): Neo4jContainer[_] = {
    val container = new Neo4jContainer(DockerImageName.parse("neo4j:4.4"))
    container.withAdminPassword(adminPassword)
    container.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    container.start()
    container
  }
}
