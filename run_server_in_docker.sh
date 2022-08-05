#!/bin/bash

docker run -p 8765:8765 -v $(greadlink --canonicalize ./data):/home/app_user/data -it joe /bin/bash
