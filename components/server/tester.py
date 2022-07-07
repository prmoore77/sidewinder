import yaml
from munch import Munch


# Read our source table shard generation info
with open("config/shard_generation_queries.yaml", "r") as data:
    TABLES = Munch(yaml.safe_load(data.read()))


