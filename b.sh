/usr/libexec/java_home -v 1.8.0
sbt assembly && scp -P 41278 target/scala-2.12/dbbench-generators-assembly-0.1.jar db-mysql-baruch-dev0a.tbd.wixprod.net:/home/avitaln/dbbench-generators.jar
