def read_ccloud_config(config_file):
    omitted_fields = set(
        ["schema.registry.url", "basic.auth.credentials.source", "basic.auth.user.info"]
    )
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                if parameter not in omitted_fields:
                    conf[parameter] = value.strip()
    return conf
