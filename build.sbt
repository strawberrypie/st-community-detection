name := "ST-detection"

version := "1.0"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-graphx"  % sparkVersion,

  "com.github.nscala-time" %% "nscala-time" % "2.16.0",

  "com.github.davidmoten" % "fastdtw" % "0.1"
)

assemblyJarName in assembly := "st-community-detection.jar"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}