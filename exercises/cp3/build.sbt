name := "Simple Project"
version := "1.0"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-graphx" % "3.1.2",
  "org.scalanlp" %% "breeze-viz" % "2.0-RC3",
  "org.scalanlp" %% "breeze" % "2.0-RC3",
  "org.jfree" % "jcommon" % "1.0.24",
  "org.jfree" % "jfreechart" % "1.5.3",
  "org.graphstream" % "gs-core" % "2.0",
  "network.aika" % "gs-ui-swing" % "2.0.1"
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}