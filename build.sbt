name := "thebigmoodometer"

version <<= version in ThisBuild

scalaVersion := "2.11.8"

val configVersion = "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % configVersion
)

publishTo := Some(Resolver.file("file",  new File( Path.userHome.absolutePath+"/my-maven-repo" )) )

coverageMinimum := 80

coverageFailOnMinimum := true