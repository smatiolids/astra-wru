source .env
../cql-proxy/cql-proxy --astra-bundle $ASTRA_BUNDLE_LOCATION --username $ASTRA_CLIENT_ID --password $ASTRA_CLIENT_SECRET --track-usage --usage-keyspace $ASTRA_KEYSPACE