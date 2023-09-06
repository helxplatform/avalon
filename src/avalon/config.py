import lakefs_client
import yaml


class Config:
    def __init__(self, lakefs_conf_path: str, metafilename: str, temp_dir: str = None):
        self.lakefs_conf_path = lakefs_conf_path
        self.temp_dir = temp_dir
        self.metafilename = metafilename

    def get_config(self):
        configuration = lakefs_client.Configuration()
        if self.temp_dir:
            configuration.temp_folder_path = self.temp_dir
        with open(self.lakefs_conf_path) as stream:
            config_raw = yaml.load(stream, Loader=yaml.FullLoader)

        configuration.username = config_raw["credentials"]["access_key_id"]
        configuration.password = config_raw["credentials"]["secret_access_key"]
        configuration.host = config_raw["server"]["endpoint_url"]
        return configuration


