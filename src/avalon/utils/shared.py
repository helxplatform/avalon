from avalon.operations.LakeFsWrapper import LakeFsWrapper
import lakefs_client
import yaml


def init_client(lakefs_conf_path, temp_dir=None):

    configuration = lakefs_client.Configuration()
    if temp_dir:
        configuration.temp_folder_path = temp_dir
    with open(lakefs_conf_path) as stream:
        config_raw = yaml.load(stream, Loader=yaml.FullLoader)

    configuration.username = config_raw["credentials"]["access_key_id"]
    configuration.password = config_raw["credentials"]["secret_access_key"]
    configuration.host = config_raw["server"]["endpoint_url"]
    the_lake = LakeFsWrapper(configuration=configuration)
    return the_lake
