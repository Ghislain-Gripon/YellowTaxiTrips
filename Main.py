#!/usr/bin/python
#-*- coding: utf-8 -*-

import logging
from FolderStructureAWS import FolderStructureAWS
from FolderStructure import FolderStructure
from FolderStructureLocal import FolderStructureLocal
from Workflow import Workflow

def lambda_handler(event, context):
    
    logging.basicConfig(format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s', level=logging.INFO)
    logging.info("Launching configuration procedures.")

    FileHandler:FolderStructure = FolderStructureAWS(config_bucket = "postgretaxiconfig", config_file_path = "config/config.yaml",
        event = event)
    Workflow(FileHandler)
    

if __name__ == "__main__":
# execute only if run as a script
    FileHandler:FolderStructure = FolderStructureLocal(config_file_path = "config/config.yaml")
    Workflow(FileHandler)



