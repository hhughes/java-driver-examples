set -x

DB_ID=$1
ASTRACS_TOKEN=$2

curl --silent --location --request POST "https://api.test.cloud.datastax.com/v2/databases/${DB_ID}/secureBundleURL" \
  --header 'Content-Type: application/json' \
  --header "Authorization: Bearer ${ASTRACS_TOKEN}" | jq -r .downloadURL | xargs wget -O ./secure-connect-bundle.zip