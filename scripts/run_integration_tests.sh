#!/bin/bash

set -e

pushd /tmp

rm -rf integration_test
mkdir integration_test
pushd integration_test

# Bootstrap the environment
sidewinder-bootstrap --client-username=scott --client-password=tiger --worker-password=united --tpch-scale-factor=1 --shard-count=11

# Start the server
sidewinder-server &

# Wait for the server to initialize
sleep 1

# Start the workers
for x in {1..11}:
do
  sidewinder-worker --tls-roots=tls/server.crt --password=united &
done

# Wait for all workers to start up
sleep 5

# Run a non-distributed query integration test to ensure all is working properly
sidewinder-client --username scott --password tiger --no-tls-verify << EOF
SELECT * FROM region;
EOF

echo "Non-Distributed query test case passed"

# Run a distributed query integration test to ensure all is working properly
sidewinder-client --username scott --password tiger --no-tls-verify << EOF
SELECT COUNT(*) AS x FROM (SELECT DISTINCT * FROM lineitem) as s;
EOF

echo "Distributed query test case passed"

popd

echo "All integration tests passed!"

# Kill all background jobs started by the script
kill $(jobs -p)

# Cleanup
rm -rf integration_test

exit 0
