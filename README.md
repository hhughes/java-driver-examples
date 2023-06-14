Sample applications that connect to Astra DB using DataStax java-driver

Prequesites:
- Java JDK8 or greater
- maven (https://maven.apache.org/index.html)
- Astra DB instance (https://astra.datastax.com)
- astraSecureConnectBundle ZIP & access token (downloaded from Astra dashboard)

Build:

    mvn compile

Run:

    mvn exec:java \
      -Dexec.mainClass=com.datastax.astra.driver.examples.CreateMoviesTable \
      -Dexec.args='--astraSecureConnectBundle=/path/to/secure-connect-astra.zip \
        --astraToken=AstraCS:... \
        --keyspace=...'
      
