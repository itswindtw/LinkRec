name := "LinkRec"
version := "1.0"
scalaVersion := "2.10.4"

seq(bintrayResolverSettings:_*)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.0.0",
  "org.apache.hbase" % "hbase-server" % "1.0.0",
  "org.apache.hbase" % "hbase-protocol" % "1.0.0",
  "org.apache.hadoop" % "hadoop-common" % "2.5.2",
  "org.apache.hadoop" % "hadoop-client" % "2.5.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
  case entry => {
    val strategy = mergeStrategy(entry)
      if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
      else strategy
  }
}}
