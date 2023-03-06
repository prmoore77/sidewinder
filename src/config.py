import os
import sys
import logging

# Setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %Z',
                    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
                    stream=sys.stdout
                    )

logger = logging.getLogger()
