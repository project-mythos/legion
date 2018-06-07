

lazy val pbs = PB.targets in Compile := Seq(
  scalapb.gen(grpc=false) -> (sourceManaged in Compile).value
)



lazy val pbsrc = PB.protoSources in Compile := Seq(file("legion-proto") )


val coreDeps = Seq(
  "demons" %% "enkidu" % "0.3-SNAPSHOT",
  "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)


lazy val commonSettings = Seq(
  organization := "demons",
  version := "0.2.0-SNAPSHOT",
  scalaVersion := "2.12.4",
  pbs,
 // pbsrc,
  autoAPIMappings := true
  //scalacOptions in (Compile,doc) := Seq("-groups", "-implicits")
)


lazy val proto = ( project in file("legion-proto") )
  .settings(
    name := "legion-proto",
    commonSettings
  ) 


lazy val core = (project in file("legion-core") )
  .settings(
    name := "legion-core",
    libraryDependencies ++= coreDeps,
    commonSettings
  ).dependsOn(proto)

/*
lazy val membership = (project in file("legion-membership") )
  .settings(
    name := "legion-membership",
    commonSettings
  ).dependsOn(core)
*/

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(name := "legion-all")
  .enablePlugins(ScalaUnidocPlugin)
  .aggregate(core)
