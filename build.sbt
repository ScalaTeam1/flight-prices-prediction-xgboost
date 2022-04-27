name := "flight-price-core"

version := "0.1"

scalaVersion := "2.12.8"

scalacOptions in (Compile, doc) ++= Seq(
  "-groups",
  "-implicits",
  "-deprecation",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)
scalacOptions ++= Seq("-encoding", "UTF-8")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "ml.dmlc" %% "xgboost4j" % "1.5.2",
  "ml.dmlc" %% "xgboost4j-spark" % "1.5.2"
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
//https://mvnrepository.com/artifact/org.mongodb.scala/mongo-scala-driver
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "4.2.3"
//AWS S3 Bucket
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.12.130"

libraryDependencies += "com.phasmidsoftware" %% "tableparser" % "1.0.14"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"

libraryDependencies += "io.jvm.uuid" %% "scala-uuid" % "0.3.1"
libraryDependencies += "org.zeroturnaround" % "zt-zip" % "1.15"
libraryDependencies += "io.minio" % "minio" % "6.0.13"
libraryDependencies += "commons-io" % "commons-io" % "2.5"
