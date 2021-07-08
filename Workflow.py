#!/usr/bin/python
#-*- coding: utf-8 -*-
import logging, DBServerError, re, pathlib, typing
from Decorator import logging_decorator
from PostgreDBServer import PostgreDBServer
from FolderStructure import FolderStructure
from getpass import getpass

#class description here
#Workflow handles the flow execution according to the flow yaml file
#
#
#
#

class Workflow:

    @logging_decorator
    def __init__(self, _FileHandler:FolderStructure):
        self.FileHandler = _FileHandler

        self.config = self.FileHandler.get_config('config')
        self.flows = self.FileHandler.get_config('flows')
        self.db_server = None
        self.run_flow()   
    
    @logging_decorator
    def run_flow(self, ) -> None:
        logging.debug("Running flows from flow file.")

        self.db_server = self.get_connection()
        now = self.db_server.get_now()
        
        logging.info("Iterating over flows.")
        for flow in self.flows["flows"]:
            logging.info("Running {} flow.".format(flow["name"]))

            flow_type = flow["type"]
            flow['now'] = now
            logging.debug("Flow of type : {}".format(flow_type))

            if flow_type == "file_to_rv":

                logging.debug("Fetching inbound file list.")
                file_list:typing.List[pathlib.Path] = self.FileHandler.get_Inbound_List(flow["file_regex"])

                for file in file_list:
                    file_new_path = ""
                    try:
                        match = re.search(flow["file_regex"], file.name)
                        flow['ext'] = match.group('ext')
                    except AttributeError:
                        logging.error("Incorrect format for the filename: {}".format(file))
                    
                    try:
                        file_new_path = self.FileHandler.Move_To_Directory(file, 'work')                 
                        try:
                            self.db_server.copy_from(self.config["db_info"]["table_name"].format_map(flow), file_new_path, flow['ext'])
                                
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
                            self.FileHandler.Move_To_Directory(file_new_path, 'done')
    
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
                        sql_script_path = self.FileHandler.get_file(self.config["data_directory_path"]["sql_scripts"] + "/" + sql_script)
                        with open(sql_script_path, 'r') as f:
                            self.db_server.execSQL(f.read().format_map(flow))
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
    @staticmethod
    def request_info() -> typing.Tuple[str,str]:
        logging.debug("Workflow.request_info starting.")
        username:str = input('Username: ')
        password:str = getpass()
        logging.debug("Workflow.request_info with username : {}".format(username))
        return username, password

    @logging_decorator
    def get_connection(self, ):
        logging.debug("Workflow.get_connection with {} arguments.".format(self, self.config))

        db_server = None
        _continue = True

        while _continue:
            try:
                username, password = self.request_info()
                logging.debug("Fetching database handling class at {}.{} using the configuration.".format(self.config["db_info"]["engine"], self.config["db_info"]["engine"]))
                
                try:
                    db_server = PostgreDBServer(self.config, password, username)
                except DBServerError.OperationalError as err:
                    logging.error("Error type : " + type(err).__name__, err.args)  
                    raise DBServerError.OperationalError("Could not establish connection, {}".format(err.args))                             

                _continue = False
                
            except Exception as err:
                logging.error("Connection error on {} with {} error.".format(self, err))
                print('Wrong username and password combination.')

        logging.debug("Workflow.get_connection ended with {} return variables.".format([self, db_server]))
        return db_server