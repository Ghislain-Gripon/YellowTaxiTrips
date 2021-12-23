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
        

        self.db_server:DBServer = self.get_db_server_class()(self.config)
        now = self.db_server.get_now()
        
        logging.info("Iterating over flows.")
        for flow in self.flows.get("flows"):
            logging.info("Running {} flow.".format(flow.get("name")))

            flow['now'] = now
            flow['origin'] = self.config.get("data_origin")
            flow['hash_func'] = self.config.get("db_info").get("hash").get("func")
            flow['hash_param'] = self.config.get("db_info").get("hash").get("param")
            logging.info("Flow of type : {}".format(flow.get('type')))

            if flow.get('type') == "file_to_rv":

                    file_list = self.FileHandler.get_Inbound_List(flow.get("file_regex"))

                    for file in file_list:
                        file_new_path = ""
                        match = re.search(flow.get("file_regex"), file.name)
                        if match:
                            flow['ext'] = match.group('ext')
                            try:
                                file_new_path = self.FileHandler.Move_To_Directory(file, 'work')                 
                                try:
                                    self.db_server.copy_from(self.config.get("db_info").get("table_name").format_map(flow), file_new_path)
                                        
                                except DBServerError.DataError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.DatabaseError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.OperationalError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.ProgrammingError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.InternalError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.IntegrityError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except DBServerError.DBError as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)  
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                except Exception as err:
                                    logging.error("Error type : " + type(err).__name__, err.args)
                                    self.FileHandler.Move_To_Directory(file_new_path, 'error')

                                else:
                                    logging.info("Copy of {} completed.".format(file_new_path))
                                    self.FileHandler.Move_To_Directory(file_new_path, 'done')
        
                            except FileNotFoundError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)
                                
                            except PermissionError as err:
                                logging.error("Error type : " + type(err).__name__, err.args)

                            except Exception as err:
                                logging.error("Error type : " + type(err).__name__, err.args)

            elif flow.get('type') == "inner_database_flux":
                
                sql_script_path = ""
                for sql_script in flow.get("sql"):
                    try:

                        environment_specific = self.config.get('data_directory_path').get('config').get('directories').get('environment_specific')
                        if environment_specific is None:
                            environment_specific = ''
                        else:
                            environment_specific += '/'

                        sql_script_path = '{}/{}'.format(self.FileHandler.sql_scripts_path, sql_script.format(environment_specific = environment_specific))
                        sql_script_file = self.FileHandler.load(sql_script_path)
                        
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
                        logging.error(err)

            else:
                logging.warning("Flow type is not recognized.")
                raise TypeError("Unrecognized flow type. {} is not known.".format(flow.get('type')))

        self.db_server.closeConn()

    @logging_decorator
    def get_db_server_class(self, ) -> DBServer:
        """
        retrieve the database class object for instanciation
        """
                
        try:
            return {
                'PostgreDBServer': PostgreDBServer,
                'RedshiftDBServer': RedshiftDBServer
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

    