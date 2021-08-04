#!/usr/bin/python
#-*- coding: utf-8 -*-
from io import FileIO
import pathlib, logging, logging.config, yaml, typing, boto3, urllib, re
from Decorator import logging_decorator
from FolderStructure import FolderStructure

#class description here
#FolderStructure handles all the operation on the file system used, it serves as the interface to it, allowing for different implementations
#depending on the file system used, be it windows, linux or AWS, ect....
#It has functions to fetch the general configuration file, the logger one and the flows.
#Another to get a file, returning its path, in a local file system it matches against the root path, in AWS it would
#request the file to be copied locally then return the path to the copy from the S3 bucket

#class version : local file system
class FolderStructureAWS(FolderStructure):

    @logging_decorator
    def __init__(self, environment:str, **kwargs):

        self.flows = None
        self.config = None
        self.config = self.get_file(kwargs['config_bucket'], kwargs['config_file_path'])['execution_environment'][environment]
        
        if(self.config is None):
            logging.error("Configuration dictonnary is null, check file location at {}".format(self.config_file_path))
            raise ValueError("Configuration dictonnary is null on {} instance.".format(self))

        logger_config_directory = self.config['data_directory_path']['config']['directories']['config']
        logger_config_filename = self.config['data_directory_path']['config']['files']['logger_config_path']
        
        logging.config.dictConfig(self.read_yaml(self.config_bucket, '{}/{}'.format(logger_config_directory, logger_config_filename) ))

        logging.info("Loaded logger yaml configuration at {}".format('{}/{}'.format(logger_config_directory, logger_config_filename) ))

        flows_config_directory = self.config['data_directory_path']['config']['directories']['flows']
        flows_config_filename = self.config['data_directory_path']['config']['files']['flows_path']

        self.flows = self.read_yaml(kwargs['config_bucket'], '{}/{}'.format(flows_config_directory, flows_config_filename))

        if(self.flows is None):
            logging.error("Flows dictonnary is null, check file location at {}".format('{}/{}'.format(flows_config_directory, flows_config_filename) ))
            raise ValueError("Flows dictonnary is null on {} instance.".format(self))

        self.bucket = kwargs['event']['Records'][0]['s3']['bucket']['name']
        self.key = urllib.parse.unquote(kwargs['event']['Records'][0]['s3']['object']['key'])

    #file_path is the pathlib.Path object to the file that is to be moved
    #directory_name is the nmae of the directory the file is to be moved to among those in
    #config["data_directory_path"]["directories"], so inbound, work, error, done
    @logging_decorator
    def Move_To_Directory(self, file_path:str, directory_name:str) -> pathlib.Path:
        moved_file = None
        try:
            file_path:str = urllib.parse.unquote(file_path)
            raw_key:str = pathlib.Path(file_path).name    
            s3 = boto3.client(service_name='s3',region_name='eu-west-3')
            s3.copy_object(
                Bucket= self.bucket,
                CopySource= '{}/{}'.format(self.bucket, file_path),
                Key= '{}/{}'.format(directory_name, raw_key),
            )
            s3.delete_object(
                Bucket= self.bucket,
                Key= file_path
            )

            moved_file:str = '{}/{}/{}'.format(self.bucket, directory_name, raw_key)
        
        except Exception as e:
            print("Error on move_directory({},{},{})".format(self.bucket, directory_name, file_path))
            print(e)
            raise e
        
        return pathlib.Path(moved_file)

    #Main function of the class, enacts all its duties of class instancing and call making.
    @logging_decorator
    def load(self, file_path):
        file_streaming:typing.BinaryIO = None
        raw_keys = pathlib.Path(urllib.parse.unquote(file_path)).parts
        directory:str = raw_keys[1]
        raw_key:str = raw_keys[2]
        bucket:str = raw_keys[0]

        try:
            s3 = boto3.client(service_name='s3',region_name='eu-west-3')
            file_streaming:typing.BinaryIO = s3.get_object(
                Bucket= bucket,
                Key= '{}/{}'.format(directory,raw_key)
                )['Body']

        except Exception as e:
            logging.error("Error on load({})".format(file_path))
            logging.error(e)
            raise e
        
        if raw_key.suffix == '.yaml':
            return self.read_yaml(file_streaming)
        else:
            return file_streaming.read()

    #read the config from disk in local directory specified in class attribute file_path
    @logging_decorator
    def read_yaml(self, file_stream) -> dict:
        _file = None
        try:
            _file = yaml.load(file_stream, Loader=yaml.SafeLoader)
            logging.info("YAML configuration file successfully read.")
        #catch a yaml related error to inform user of problem with config file
        
        except Exception as e:
            logging.error(str(e) + ", {}".format(file_stream))
            _file = None
            raise e
        return _file

    @logging_decorator
    def get_config(self, ) -> dict:
        return self.config

    @logging_decorator
    def get_flows(self, ) -> dict:
        return self.flows
    
    @logging_decorator
    def get_Inbound_List(self, regex):
        file_list = None
        if re.search(self.key, regex) is not None:
            file_list = ['{}/{}'.format(self.bucket, self.key)]
        logging.info("Fetching list of files in inbound.")
        return file_list