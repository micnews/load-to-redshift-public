name := "LoadToRedshift"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2", // "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.0.2", // "2.1.0",
  "com.databricks" %% "spark-redshift" % "2.0.1", // "3.0.0-preview1",
  "com.amazon.redshift" % "redshift-jdbc42" % "1.2.10.1009",
  "com.amazonaws" % "aws-java-sdk-redshift" % "1.11.109",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.304",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.304",
  "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.310",
  "com.typesafe" % "config" % "1.3.1",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "6.2.3",
  "com.sksamuel.elastic4s" %% "elastic4s-json4s" % "6.2.3",
  "com.sksamuel.elastic4s" %% "elastic4s-circe" % "6.2.3",
  "com.sksamuel.elastic4s" %% "elastic4s-aws" % "6.2.3",
  "com.sksamuel.elastic4s" %% "elastic4s-http" % "6.2.3"
)

resolvers += "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"

enablePlugins(DockerPlugin)

dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("openjdk:8-jre")
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", artifactTargetPath)
  }
}

mainClass in assembly := Some("Main")

assemblyMergeStrategy in assembly := {
  case PathList("lib", "static", "Windows", xs @ _*) => MergeStrategy.discard
  case PathList("lib", "static", "Mac OS X", xs @ _*) => MergeStrategy.discard
  case PathList("com", "amazon", xs @ _*) => MergeStrategy.first
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.first
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case PathList("com", "sun", xs @ _*) => MergeStrategy.first
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.first
  case PathList("commons-beanutils", xs @ _*) => MergeStrategy.first
  case PathList("io", "netty", xs @ _*) => MergeStrategy.first
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.first
  case PathList("net", "java", xs @ _*) => MergeStrategy.first
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.first
  case PathList("org", "glassfish", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.first
//  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.first
//  case "META-INF/mailcap" => MergeStrategy.first
//  case "META-INF/mimetypes.default" => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "log4j.properties" => MergeStrategy.first
  case "overview.html" => MergeStrategy.first
  case "plugin.properties" => MergeStrategy.first
//  case x if x.endsWith("META-INF/org.apache.arrow.versions.properties") ⇒ MergeStrategy.first
  case x if x.endsWith("META-INF/io.netty.versions.properties") ⇒ MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}