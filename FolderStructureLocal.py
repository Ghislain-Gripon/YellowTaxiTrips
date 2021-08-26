#!/usr/bin/python
#-*- coding: utf-8 -*-
import pathlib
import logging
import logging.config
import yaml
import re
from FolderStructure import FolderStructure
from Decorator import logging_decorator

#class description here
#FolderStructure handles all the operation on the file system used, it serves as the interface to it, allowing for different implementations
#depending on the file system used, be it windows, linux or AWS, ect....
#It has functions to fetch the general configuration file, the logger one and the flows.
#Another to get a file, returning its path, in a local file system it matches against the root path, in AWS it would
#request the file to be copied locally then return the path to the copy from the S3 bucket

#class version : local file system
class FolderStructureLocal(FolderStructure):

    @logging_decorator
    def __init__(self, **kwargs):
        self.root_path:pathlib.Path = kwargs.get('root_path')
        if self.root_path is None:
            self.root_path:pathlib.Path = pathlib.Path.cwd()
        self.config_file_path:str = kwargs.get('config_file_path')
        if self.config_file_path is None:
            self.config_file_path:pathlib.Path = pathlib.Path('config/config.yaml')
        self.file_directories = {}
        self.config:dict = self.load(self.config_file_path)['execution_environment']['local']
        self.sql_scripts_path:str = '{}'.format(self.config['data_directory_path']['config']['directories']['sql_scripts'])
        
        if(self.config is None):
            logging.error("Configuration dictonnary is null, check file location at {}".format(self.config_file_path))
            raise ValueError("Configuration dictonnary is null on {} instance.".format(self))
            
        logging.config.dictConfig(self.load('{}/{}'.format(self.config['data_directory_path']['config']['directories']['config'], 
            self.config['data_directory_path']['config']['files']['logger_config_path'])))

        logging.info("Loaded logger yaml configuration at {}".format('{}/{}'.format(self.config['data_directory_path']['config']['directories']['config'], 
            self.config['data_directory_path']['config']['files']['logger_config_path'])))

        self.flows:dict = self.load('{}/{}'.format(self.config['data_directory_path']['config']['directories']['flows'], 
            self.config['data_directory_path']['config']['files']['flows_path']))

        if(self.flows is None):
            logging.error("Flows dictonnary is null, check file location")
            raise ValueError("Flows dictonnary is null.")
            
        for directory in self.config["data_directory_path"]['data']["directories"]:
            file_directory:pathlib.Path = self.root_path / pathlib.Path(self.config["data_directory_path"]['data']["base_path"]) / directory
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
    def get_Inbound_List(self, regex):
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
    def load(self, file_path:str):
        _file = None
        path:pathlib.Path = pathlib.Path(self.root_path) / file_path
        if self._check_for_file(path):    
            _file = open(str(path), 'r')
        else:
            logging.warning("No file located at {}".format(file_path))
            raise FileNotFoundError("There is no file at {}".format(path))

        if pathlib.Path(file_path).suffix == '.yaml':
            return self.read_yaml(_file)
        else:
            return _file.read()
        

    #read the config from disk in local directory specified in class attribute file_path
    @logging_decorator
    def read_yaml(self, file_stream) -> dict:
        """
        Safe loads a yaml dictionary from an open file stream.
        """

        _file:dict = None

        try:
            _file:dict = yaml.load(file_stream, Loader=yaml.SafeLoader)
            logging.info("YAML configuration file successfully read.")
            
        #catch a yaml related error to inform user of problem with config file
        except FileNotFoundError:
            logging.error("YAML file at {} couldn't be decoded.".format(file_stream))
            raise FileNotFoundError("YAML file at {} couldn't be decoded.".format(file_stream))
            
        except PermissionError:
            logging.exception("Can not access {}, permission denied.".format(file_stream))
            raise PermissionError("Can not access {}, permission denied.".format(file_stream))

        except:
            logging.error("File reading error.")
            raise "File reading error."

        file_stream.close()
        return _file

    @logging_decorator
    def get_config(self, ) -> dict:
        """
        Returns the configuration dictionary.
        """
        return self.config

    @logging_decorator
    def get_flows(self, ) -> dict:
        """
        Returns the flows dictionary.
        """
        return self.flows