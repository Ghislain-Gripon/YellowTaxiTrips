class FolderStructure:

    def __init__(self, **kwargs):
        self.config_file_path = None
        self.config = None
        self.flows = None
        self.sql_scripts_path = None
        pass

    def Move_To_Directory(self, file_path:str, directory_name:str):
        pass

    def load(self, file_path:str):
        pass

    def read_yaml(self, file_stream) -> dict:
        pass

    def get_inbound(self, regex:str):
        pass

    def get_config(self, ) -> dict:
        """
        Returns the configuration dictionary.
        """
        pass

    def get_flows(self, ) -> dict:
        """
        Returns the flows dictionary.
        """
        pass