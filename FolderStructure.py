#!/usr/bin/python
#-*- coding: utf-8 -*-
import pathlib, logging, logging.config, yaml, re, typing, boto3, urllib, botocore, json
from Decorator import logging_decorator

#class description here
#FolderStructure handles all the operation on the file system used, it serves as the interface to it, allowing for different implementations
#depending on the file system used, be it windows, linux or AWS, ect....
#It has functions to fetch the general configuration file, the logger one and the flows.
#Another to get a file, returning its path, in a local file system it matches against the root path, in AWS it would
#request the file to be copied locally then return the path to the copy from the S3 bucket

#class version : local file system
class FolderStructure:

    config_bucket = "postgretaxiconfig"
    config_file_path = "config/config.yaml"

    @logging_decorator
    def __init__(self, ):

        self.config = self.get_file(self.config_bucket, self.config_file_path)['execution_environment'][self.config['environment']]
        self.flows = None
        
        if(self.config is None):
            logging.error("Configuration dictonnary is null, check file location at {}".format(self.config_file_path))
            raise ValueError("Configuration dictonnary is null on {} instance.".format(self))

        logger_config_directory = self.config['data_directory_path']['config']['directories']['config']
        logger_config_filename = self.config['data_directory_path']['config']['files']['logger_config_path']
        
        logging.config.dictConfig(self.read_yaml(self.config_bucket, '{}/{}'.format(logger_config_directory, logger_config_filename) ))

        logging.info("Loaded logger yaml configuration at {}".format('{}/{}'.format(logger_config_directory, logger_config_filename) ))

        flows_config_directory = self.config['data_directory_path']['config']['directories']['flows']
        flows_config_filename = self.config['data_directory_path']['config']['files']['flows_path']

        self.flows = self.read_yaml(self.config_bucket, '{}/{}'.format(flows_config_directory, flows_config_filename))

        if(self.flows is None):
            logging.error("Flows dictonnary is null, check file location at {}".format('{}/{}'.format(flows_config_directory, flows_config_filename) ))
            raise ValueError("Flows dictonnary is null on {} instance.".format(self))

    #file_path is the pathlib.Path object to the file that is to be moved
    #directory_name is the nmae of the directory the file is to be moved to among those in
    #config["data_directory_path"]["directories"], so inbound, work, error, done
    @logging_decorator
    def Move_To_Directory(self, bucket:str, directory:str, key:str) -> pathlib.Path:
        moved_file = None
        try:
            key:str = urllib.parse.unquote(key)
            raw_key:str = pathlib.Path(key).name    
            s3 = boto3.client(service_name='s3',region_name='eu-west-3')
            s3.copy_object(
                Bucket= bucket,
                CopySource= '{}/{}'.format(bucket, key),
                Key= '{}/{}'.format(directory, raw_key),
            )
            s3.delete_object(
                Bucket= bucket,
                Key= key
            )

            moved_file:str = '{}/{}/{}'.format(bucket, directory, raw_key)
        
        except Exception as e:
            print("Error on move_directory({},{},{})".format(bucket, directory, key))
            print(e)
            raise e
        
        return pathlib.Path(moved_file)

    #Main function of the class, enacts all its duties of class instancing and call making.
    @logging_decorator
    def load(self, bucket, key):
        _file = None
        try:
            raw_key = urllib.parse.unquote(key)
            s3 = boto3.client(service_name='s3',region_name='eu-west-3')
            file_streaming = s3.get_object(
                Bucket= bucket,
                Key= raw_key
                )['Body']
            
            _file = file_streaming

        except Exception as e:
            print("Error on load({},{})".format(bucket, key))
            print(e)
            raise e

        return _file

    #read the config from disk in local directory specified in class attribute file_path
    @logging_decorator
    def read_yaml(self, bucket, key):
        _file = None
        try:
            _file = yaml.load(self.load(bucket, key), Loader=yaml.SafeLoader)
            logging.info("YAML configuration file successfully read.")
        #catch a yaml related error to inform user of problem with config file
        
        except Exception as e:
            logging.error(str(e) + ", ({},{},{})".format(key, bucket))
            _file = None
            raise e
        return _file

    @logging_decorator
    def get_config(self, config_type):
        config = None
        if config_type == "config":
            config = self.config
        if config_type == "flows":
            config = self.flows
        return config

    @logging_decorator
    def get_file(self, bucket, key):
        _file = self.load(bucket, key).read()
        return _file