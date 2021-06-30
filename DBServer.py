#!/usr/bin/python
#-*- coding: utf-8 -*-

#IMPORTANT any child class should have the file name that acts as module name identical to the class name that inherits
#the abstracts DBServer class.
#This allows for dynamic import of the right database handling class in the Treatment.Treatment class according to the value
#of the db_info.engine field of the config configuration file.
#Due to how the importlib library works, to import the the database handling class and its functions it is required to
#use getattr(import_module(config["db_info"]["engine"]),config["db_info"]["engine"])
class DBServer:
    
    def __init__(self, _config, _user):
        self.endpoint = _config["db_info"]["server"]
        self.db_name = _config["db_info"]["name"]
        self.db_user = _user
        self.port = _config["db_info"]["port"]
        

    def execSQL(self, query) -> None:
        #execute on self.conn database the query
        pass

    def closeConn(self, ) -> None:
        #close the connection to db.conn database server
        pass

    def copy_from(self, table_name, data_path) -> None:
        #use the database file to server functionnality to get file at data_path in table_name
        #using the database at self.conn connection
        pass

    def get_now(self, ) -> str:
        #gets the current time from the database.
        pass


    

