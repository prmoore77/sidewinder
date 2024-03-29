#!/bin/bash

OS_PLATFORM=$(uname)
if [ "${OS_PLATFORM}" == "Darwin" ];
then
  READLINK_COMMAND="greadlink"
else
  READLINK_COMMAND="readlink"
fi

docker run --name sidewinder-server \
       --rm \
       --publish 8765:8765 \
       --volume $(${READLINK_COMMAND} --canonicalize ./data):/opt/sidewinder/data \
       --interactive \
       --tty \
       prmoorevoltron/sidewinder:latest \
       sidewinder-server
