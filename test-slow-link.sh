SCB=$1
AT=$2
KEYSPACE=$3

if [ -z ${SCB} ] || [ -z ${AT} ] || [ -z ${KEYSPACE} ]; then
  echo 'Usage: test-slow-link.sh /path/to/SECURE_CONNECT_BUNDLE $ASTRA_TOKEN $KEYSPACE'
  exit 1
fi

echo "Using secure connect bundle: ${SCB}"
echo "Using astra token: ${AT}"
echo "Using keyspace: ${KEYSPACE}"

# copy SCB into local directory
cp ${SCB} ./secure-connect-bundle.zip

MVN_ARGS="-Dargs.astraSecureConnectBundle='/secure-connect-bundle.zip' -Dargs.astraToken='${AT}' -Dargs.keyspace='${KEYSPACE}'"

docker build --no-cache -t java-driver-examples:1 -f ./Dockerfile .
#docker run -it --cap-add NET_ADMIN java-driver-examples:1 /bin/sh
#docker run -it --rm java-driver-examples:1 mvn test -Dargs.astraSecureConnectBundle="/secure-connect-bundle.zip" -Dargs.astraToken="${AT}" -Dargs.keyspace="${KEYSPACE}"
docker run -it --cap-add NET_ADMIN --rm java-driver-examples:1 /bin/bash -c "mvn test ${MVN_ARGS} \
  && tc qdisc add dev eth0 root netem delay 5000ms 3000ms \
  && mvn --offline test ${MVN_ARGS}"