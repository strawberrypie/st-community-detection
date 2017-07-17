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

assemblyJarName in assembly := "st-community-detection.jar"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}