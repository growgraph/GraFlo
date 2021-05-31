import yaml
from wos_db_studies.utils_json import parse_edges, get_json_data, foo_parallel
from collections import defaultdict
from os.path import expanduser
from timeit import default_timer

sources = [
    expanduser(
        "~/data/wos/experiment/tmp/1980/WR_1980_20190212023637_DSSHPSH_0001#good#0.json.gz"
    ),
    expanduser("~/data/wos/experiment/tmp/1985/dump_xml_0#good#0.json.gz"),
    expanduser(
        "~/data/wos/experiment/tmp/2010/WR_2010_20190215011716_DSSHPSH_0001#good#0.json.gz"
    ),
]


config_path = "../conf/wos_json.yaml"

with open(config_path, "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)
index_fields_dict = {k: v["index"] for k, v in config["vertex_collections"].items()}

all_fields_dict = {k: v["fields"] for k, v in config["vertex_collections"].items()}

edge_des, excl_fields = parse_edges(config["json"], [], defaultdict(list))


# parallelize
kwargs = {
    "config": config["json"],
    "vertex_config": config["vertex_collections"],
    "edge_fields": excl_fields,
    "merge_collections": ["publication"],
}

for source in sources:
    print(source)
    data = get_json_data(source)
    print(len(data))
    begin = default_timer()
    foo_parallel(data, kwargs, 1000)
    end = default_timer()
    print(f"{end - begin:.3g} sec")
