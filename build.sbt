name := "ST-detection"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-graphx"  % "2.1.1",

  "com.github.nscala-time" %% "nscala-time" % "2.16.0",

  "ml.sparkling" %% "sparkling-graph-operators" % "0.0.8-SNAPSHOT",
  "com.github.davidmoten" % "fastdtw" % "0.1"
)