import os
import sys
import logging

# Setup logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
                    stream=sys.stdout
                    )

logger = logging.getLogger()
