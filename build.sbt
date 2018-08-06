
PB.targets in Compile := Seq(
  scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
)

organization := "demons"
name := "legion"

version := "0.3-SNAPSHOT"


scalacOptions in Compile ++= Seq(
  "-feature",
  "-language:postfixOps"
)

val enkiVersion = "0.3-SNAPSHOT"

libraryDependencies ++= Seq(
  "demons" %% "enki-crdt" % enkiVersion,
  "demons" %% "enki-routing" %  enkiVersion,
  "demons" %% "enkidu" % "0.3-SNAPSHOT",
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)



fork in run := true 
