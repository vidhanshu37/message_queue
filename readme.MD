# Terminal 1
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=8082 -Dbroker.id=broker-1 -Dmq.data.dir=C:/tmp/mq-data-1"

# Terminal 2
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=8083 -Dbroker.id=broker-2 -Dmq.data.dir=C:/tmp/mq-data-2"

# Terminal 3
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=8084 -Dbroker.id=broker-3 -Dmq.data.dir=C:/tmp/mq-data-3"
