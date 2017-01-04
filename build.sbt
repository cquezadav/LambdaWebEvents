name := "WebEvents"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++=Seq(
	"com.typesafe"					% 	"config"							% 	"1.3.0",
    "org.apache.kafka"              %   "kafka-clients"               		%   "0.9.0.1",
    "com.typesafe.play"             %   "play-json_2.11"              		%   "2.4.6",
    "com.google.guava"           	%   "guava"                       		%   "19.0",
    //"com.datastax.cassandra" 		% 	"cassandra-driver-core" 			% 	"3.0.2",
    "com.google.code.gson" 			% 	"gson" 								% 	"2.3.1",
    "org.apache.spark" 				%% 	"spark-core" 						% 	"2.0.2" 	%	"provided",
    "org.apache.spark" 				%% 	"spark-sql" 						% 	"2.0.2",
    "com.datastax.spark" 			% 	"spark-cassandra-connector_2.11" 	% 	"2.0.0-M3"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

//assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org","apache","spark","unused","UnusedStubClass.class") => MergeStrategy.first
  case x => MergeStrategy.first
}

