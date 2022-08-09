#!/bin/bash

docker run -p 8765:8765 \
       --platform=linux/amd64 \
       --volume $(greadlink --canonicalize ./data):/home/app_user/data \
       -it \
       795371563663.dkr.ecr.us-east-2.amazonaws.com/sw:latest \
       /bin/bash
