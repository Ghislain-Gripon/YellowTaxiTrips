#!/usr/bin/python
#-*- coding: utf-8 -*-
import pathlib, logging, logging.config, yaml, re, typing
from Decorator import logging_decorator

#class description here
#FolderStructure handles all the operation on the file system used, it serves as the interface to it, allowing for different implementations
#depending on the file system used, be it windows, linux or AWS, ect....
#It has functions to fetch the general configuration file, the logger one and the flows.
#Another to get a file, returning its path, in a local file system it matches against the root path, in AWS it would
#request the file to be copied locally then return the path to the copy from the S3 bucket

#class version : local file system
class FolderStructure:

    config_file_path = "config/config.yaml"

    @logging_decorator
    def __init__(self, environment:str, **kwargs):
        self.root_path = kwargs['root_path']
        self.file_directories = {}
        self.config = self.load(self.get_file(self.config_file_path))['execution_environment'][environment]
        self.flows = None
        
        if(self.config is None):
            logging.error("Configuration dictonnary is null, check file location at {}".format(self.config_file_path))
            raise ValueError("Configuration dictonnary is null on {} instance.".format(self))
        with open(self.config["logger_config_path"]) as f:
            logging.config.dictConfig(yaml.load(f, Loader=yaml.SafeLoader))
        logging.info("Loaded logger yaml configuration at {}".format(self.config["logger_config_path"]))
        self.flows = self.load(self.get_file(self.config["flows_path"]))
        if(self.flows is None):
            logging.error("Flows dictonnary is null, check file location at {}".format(self.config["flows_path"]))
            raise ValueError("Flows dictonnary is null on {} instance.".format(self))
        for directory in self.config["data_directory_path"]["directories"]:
            file_directory:pathlib.Path = self.root_path / pathlib.Path(self.config["data_directory_path"]["base_path"]) / directory
            pathlib.Path(file_directory).mkdir(parents=True, exist_ok=True)
            self.file_directories[directory] = file_directory
            logging.debug("File directory {} loaded.".format(file_directory))
        logging.info("File directories set up.")

    #file_path is the pathlib.Path object to the file that is to be moved
    #directory_name is the nmae of the directory the file is to be moved to among those in
    #config["data_directory_path"]["directories"], so inbound, work, error, done
    @logging_decorator
    def Move_To_Directory(self, file_path, directory_name) -> pathlib.Path:    
        moved_file = None
        try:
            file_path = pathlib.Path(file_path)
            logging.debug("Moving {} to {}".format(file_path, self.file_directories[directory_name] / file_path.name))
            pathlib.Path.rename(file_path, self.file_directories[directory_name] / file_path.name)
            moved_file = pathlib.Path(self.file_directories[directory_name] / file_path.name)
            logging.info("Moved {} to {}".format(file_path.name, moved_file.parent))
        except FileNotFoundError as err:
            logging.error("Could not move the file, {} occured.".format(err))
            raise FileNotFoundError("File not found : {}".format(err.args))
        except PermissionError as err:
            logging.error("Could not move the file, {} occured.".format(err))
            raise PermissionError("Access denied, permission failed : {}".format(err.args))
        except Exception as err:
            logging.error("An error occured : {}".format(err.args))
            raise Exception("An unknown error occured : {}".format(err.args))
        return moved_file

    #Fetch list of files in Inbound directory
    @logging_decorator
    def get_Inbound_List(self, regex) -> typing.List[pathlib.Path]:
        inbound:pathlib.Path = self.file_directories["inbound"]
        file_list = []
        logging.info("Fetching list of files in inbound.")
        [file_list.append(i) for i in inbound.iterdir() if re.search(regex, i.name) is not None]
        return file_list
    
    #Checks for a file at file_path
    @logging_decorator
    def _check_for_file(self, file_path) -> bool:
        is_file:bool = pathlib.Path.is_file(file_path)
        return is_file

    #Main function of the class, enacts all its duties of class instancing and call making.
    @logging_decorator
    def load(self, file_path) -> dict:
        file_exists:bool = self._check_for_file(file_path)
        if(not file_exists):
            logging.warning("No file located at {}".format(file_path))
            raise FileNotFoundError("{} does not exist.".format(file_path))
        logging.info("Reading {}, forwarding python yaml dict.".format(file_path))
        _file = self.read_yaml(file_path)
        return _file

    #read the config from disk in local directory specified in class attribute file_path
    @logging_decorator
    def read_yaml(self, file_path) -> dict:
        file_exists:bool = self._check_for_file(file_path)
        if(not file_exists):
            logging.warning("No file located at {}".format(file_path))
            raise FileNotFoundError("{} does not exist.".format(file_path))
        logging.info("Reading {}, forwarding python yaml dict.".format(file_path))
        _file = None
        try:
            with open(file_path) as yaml_file:
                _file = yaml.load(yaml_file, Loader=yaml.SafeLoader)
            logging.info("YAML configuration file successfully read.")
        #catch a yaml related error to inform user of problem with config file
        except FileNotFoundError:
            logging.error("YAML file at {} couldn't be decoded.".format(file_path))
            _file = None
        except PermissionError:
            logging.exception("Can not access {}, permission denied.".format(file_path))
        except:
            logging.error("File reading error.")
            _file = None
        return _file

    @logging_decorator
    def get_config(self, config_type) -> dict:
        config = None
        if config_type == "config":
            config = self.config
        if config_type == "flows":
            config = self.flows
        return config

    @logging_decorator
    def get_file(self, file_name) -> pathlib.Path:
        _file = None
        path = pathlib.Path(self.root_path) / file_name
        if not self.check_for_file(path):
            raise FileNotFoundError("There is not file at {}".format(path))
        with open(path, 'r') as f:
            _file = f.read()
        return _file