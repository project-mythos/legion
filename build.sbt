PB.targets in Compile := Seq(
  scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "demons" %% "enkidu" % "0.3-SNAPSHOT",
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)


