name := "accumulo-ext-plugin"

organization := "com.schwartech"

version := "1.0-SNAPSHOT"

publishTo := Some(Resolver.file("http://jschwartz73.github.io/play2-accumulo-ext",
  new File("/Users/jeff/dev/myprojects/play2-accumulo-ext.github.com")))

libraryDependencies ++= Seq(
  "com.schwartech" %% "accumulo-plugin" % "1.0-SNAPSHOT"
)

resolvers += (
  "SchwarTech GitHub Repository" at "http://jschwartz73.github.io/play2-accumulo/"
)

play.Project.playJavaSettings
