#!/bin/bash
snapctl plugin load ../build/plugin/snap-plugin-collector-mock1
snapctl plugin load ../build/plugin/snap-plugin-publisher-mock-file
snapctl plugin load ../build/plugin/snap-plugin-processor-passthru

# create a task where user=`root` and password=`secret`; publish data to /tmp/snap_published_mock_file1.log
snapctl task create -t ./tasks/mock-file.json

# create a task where user=`root2` and password=`secret2`; publish data to /tmp/snap_published_mock_file2.log
snapctl task create -t  ./tasks/mock-file2.json

