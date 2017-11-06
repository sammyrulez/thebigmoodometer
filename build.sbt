name := "thebigmoodometer"

version <<= version in ThisBuild

scalaVersion := "2.11.8"

val configVersion = "1.3.0"
val coreNlpVersion = "3.6.0"
val sparkVersion ="2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % sparkVersion ,
  "org.apache.spark" %% "spark-sql"  % sparkVersion
)


assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName.contains("spark")}
}


libraryDependencies ++= Seq(
  "com.typesafe" % "config" % configVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % coreNlpVersion classifier "models"
)

val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

publishTo := Some(Resolver.file("file",  new File( Path.userHome.absolutePath+"/my-maven-repo" )) )

coverageMinimum := 80

coverageFailOnMinimum := true