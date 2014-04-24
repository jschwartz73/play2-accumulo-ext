name := "accumulo-ext-plugin"

organization := "com.schwartech"

version := "1.0-SNAPSHOT"


publishTo := Some(Resolver.file("http://jschwartz73.github.com/m2repo/releases/",
  new File("/Users/jeff/dev/myprojects/jschwartz73.github.com/m2repo/releases")))

libraryDependencies ++= Seq(
  "com.schwartech" %% "accumulo-plugin" % "1.0-SNAPSHOT"
)

resolvers += (
  "SchwarTech GitHub Repository" at "http://jschwartz73.github.com/m2repo/releases/"
)

play.Project.playJavaSettings
