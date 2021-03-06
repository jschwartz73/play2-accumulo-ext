name := "accumulo-ext-plugin"

organization := "com.schwartech"

version := "1.1-SNAPSHOT"

publishTo := Some(Resolver.file("http://schwartech.github.com/m2repo/releases/",
  new File("/Users/jeff/dev/myprojects/schwartech.github.com/m2repo/releases")))

libraryDependencies ++= Seq(
  "com.schwartech" %% "accumulo-plugin" % "1.1-SNAPSHOT"
)

resolvers += (
  "SchwarTech GitHub Repository" at "http://schwartech.github.com/m2repo/releases/"
)

play.Project.playJavaSettings
