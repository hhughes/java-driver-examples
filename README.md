Sample applications that connect to Astra DB using DataStax java-driver

Prequesites:
- Java JDK8 or greater
- maven (https://maven.apache.org/index.html)
- Astra DB instance (https://astra.datastax.com)
- astraSecureConnectBundle ZIP & access token (downloaded from Astra dashboard)

Build:

    mvn clean package

Run:

    mvn exec:java \
      -Dargs.astraSecureConnectBundle=/path/to/secure-connect-astra.zip \
      -Dargs.astraToken=AstraCS:... \
      -Dargs.keyspace=...
      
Simulate slow connections:

> Additional requirement: `docker`

    ./test-slow-link.sh "/path/to/secure-connect-astra.zip" "AstraCS:.." "<keyspace>"