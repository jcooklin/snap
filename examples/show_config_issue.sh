#!/bin/bash 

snapctl plugin load ../build/plugin/snap-plugin-collector-mock1
snapctl plugin load ../build/plugin/snap-plugin-publisher-mock-file
snapctl plugin load ../build/plugin/snap-plugin-processor-passthru

# create a task where user=`root` and password=`secret`; publish data to /tmp/snap_published_mock_file1.log
snapctl task create -t ./tasks/mock-file.json

# create a task where user=`root2` and password=`secret2`; publish data to /tmp/snap_published_mock_file2.log
snapctl task create -t  ./tasks/mock-file2.json

echo "sleeping for 10 seconds and then we will update the global plugin config"
sleep 10

curl -X PUT -H 'Content-Type: application/json' -d '{"file": "/tmp/snap-iza2.out"}' http://localhost:8181/v1/plugins/collector/mock-file/3/config 

