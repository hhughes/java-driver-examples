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
      -Dargs.astraSecureConnectBundle="/path/to/secure-connect-astra.zip" \
      -Dargs.astraToken="AstraCS:..." \
      -Dargs.keyspace="..."

Generate Graal native image:

> requires JAVA_HOME set to graal VM, tested with Graal CE 17.0.7

    mvn -Pnative -Dagent clean package

Run Graal native image:

    mvn -Pnative -Dagent exec:exec@native \
        -Dargs.astraSecureConnectBundle="/path/to/secure-connect-astra.zip" \
        -Dargs.astraToken="AstraCS:..." \
        -Dargs.keyspace="..."
