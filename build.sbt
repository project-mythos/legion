
PB.targets in Compile := Seq(
  scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
)

organization := "demons"

version := "0.3.0-SNAPSHOT"

scalaVersion := "2.12.4"

scalacOptions in Compile ++= Seq(
  "-feature",
  "-language:postfixOps"
)


libraryDependencies ++= Seq(
  "demons" %% "enki-crdt" % "0.3-SNAPSHOT",
  "demons" %% "enkidu" % "0.3-SNAPSHOT",
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)



