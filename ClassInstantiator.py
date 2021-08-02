#!/usr/bin/python
#-*- coding: utf-8 -*-
import FolderStructure, FolderStructureLocal, FolderStructureAWS
import DBServer, PostgreDBServer, RedshiftDBServer
import logging
from Decorator import logging_decorator

class ClassInstantiator:

    @logging_decorator
    def __init__(self) -> None:
        pass
