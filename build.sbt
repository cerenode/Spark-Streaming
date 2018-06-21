organization := "com.cerenode"
scalaVersion := "2.11.11"
version := "0.1.0"
name := "spark_streaming"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.3.1",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"
)
