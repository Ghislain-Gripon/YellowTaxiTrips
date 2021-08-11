#!/usr/bin/python
#-*- coding: utf-8 -*-
import logging
import DBServerError
import re
from Decorator import logging_decorator
from PostgreDBServer import PostgreDBServer
from RedshiftDBServer import RedshiftDBServer
from DBServer import DBServer
from FolderStructureAWS import FolderStructureAWS
from FolderStructure import FolderStructure
from FolderStructureLocal import FolderStructureLocal

#class description here
#Workflow handles the flow execution according to the flow yaml file
#
#
#
#

class Workflow:

    @logging_decorator
    def __init__(self, _FileHandler):
        self.FileHandler:FolderStructure = _FileHandler

        self.config:dict = self.FileHandler.get_config()
        self.flows:dict = self.FileHandler.get_flows()
        self.db_server:DBServer = None

        self.run_flow()
    
    @logging_decorator
    def run_flow(self,):
        logging.debug("Running flows from flow file.")
        

        self.db_server:DBServer = self.get_connection()
        now = self.db_server.get_now()
        
        logging.info("Iterating over flows.")
        for flow in self.flows["flows"]:
            logging.info("Running {} flow.".format(flow["name"]))

            file = '{}/{}'.format(self.bucket, self.key)
            flow_type = flow["type"]
            flow['now'] = now
            logging.debug("Flow of type : {}".format(flow_type))

            if flow_type == "file_to_rv":

                    file_list = self.FileHandler.get_Inbound_List(flow["file_regex"])

                    for file in file_list:
                        file_new_path = ""
                        try:
                            re.search(flow["file_regex"], file.name)
                        except AttributeError:
                            logging.error("Incorrect format for the filename: {}".format(file))
                        
                        try:
                            file_new_path = self.FileHandler.Move_To_Directory(self.bucket, 'work', self.key)                 
                            try:
                                self.db_server.copy_from(self.config["db_info"]["table_name"].format_map(flow), file_new_path)
                                    
                            except DBServerError.DataError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.DatabaseError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.OperationalError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.ProgrammingError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.InternalError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.IntegrityError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except DBServerError.DBError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)  
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            except Exception as err:
                                logging.error("Error type : " + type(err).__name__, err.args)
                                self.FileHandler.Move_To_Directory(self.bucket, 'error', file_new_path)

                            else:
                                self.FileHandler.Move_To_Directory(self.bucket, 'done', file_new_path)
    
                        except FileNotFoundError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)
                            
                        except PermissionError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)

                        except Exception as err:
                            logging.error("Error type : " + type(err).__name__, err.args)

            elif flow_type == "inner_database_flux":
                
                sql_script_path = ""
                for sql_script in flow["sql"]:
                    try:

                        sql_script_file = self.FileHandler.load('{}/{}'.format(self.FileHandler.sql_scripts_path, sql_script))
                        sql_script_path = '{}/{}'.format(self.FileHandler.sql_scripts_path, sql_script)
                        
                        self.db_server.execSQL(sql_script_file.format_map(flow))
                        logging.info("Run {} script on database.".format(sql_script_path))

                    except DBServerError.DataError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except DBServerError.DatabaseError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except DBServerError.OperationalError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))

                    except DBServerError.ProgrammingError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except DBServerError.InternalError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except DBServerError.IntegrityError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except DBServerError.DBError as err:
                        logging.error("Error with the file handling. File : {}, Error: {}".format(sql_script_path, err))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except FileNotFoundError as err:
                        logging.error("File was not found {}.".format(sql_script_path))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except PermissionError as err:
                        logging.error("Permission denied, can not access file {}.".format(sql_script_path))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

                    except Exception as err:
                        logging.error("Error with file handling at {}.".format(sql_script_path))
                        logging.error("Error type : " + type(err).__name__, str(err.args))  

            else:
                logging.warning("Flow type is not recognized.")
                raise TypeError("Unrecognized flow type. {} is not known.".format(flow_type))

        self.db_server.closeConn()

    @logging_decorator
    def get_connection(self, ) -> DBServer:
        """
        retrieve the database class instance object
        """
                
        try:
            return {
                'PostgreDBServer': PostgreDBServer(self.config),
                'RedshiftDBServer': RedshiftDBServer(self.config)
            }[self.config['db_info']['engine']]

        except DBServerError.OperationalError as err:
            logging.error("Error type : " + type(err).__name__, err.args)  
            raise DBServerError.OperationalError("Could not establish connection, {}".format(err.args))

        except ValueError as err:
            logging.error("Wrong value for database enine type from config.")
            raise ValueError("Wrong value for database enine type from config.")
            
        except Exception as err:
            logging.error("Connection error on {} with {} error.".format(self, err))
            raise Exception('Wrong username and password combination.')  

    