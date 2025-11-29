import snowflake.connector

def get_sf_connection(config):
    kwargs = {
        "user": config["user"],
        "account": config["account"],
        "warehouse": config["warehouse"],
        "database": config["database"],
        "schema": config["schema"],
        "role": config["role"],
        "authenticator": "snowflake"
    }
    if config.get("private_key_file"):
        kwargs["private_key_file"] = config["private_key_file"]

    return snowflake.connector.connect(**kwargs)
