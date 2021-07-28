#!/usr/bin/python
#-*- coding: utf-8 -*-
import logging, DBServerError, re, typing, boto3, base64, botocore
from botocore.exceptions import ClientError
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
    def __init__(self, _FileHandler, bucket, key):
        self.FileHandler = _FileHandler

        self.config = self.FileHandler.get_config('config')
        self.flows = self.FileHandler.get_config('flows')

        self.db_server = None
        self.region_name = self.config['region']

        self.run_flow(bucket, key)
    
    @logging_decorator
    def run_flow(self, bucket, key):
        logging.debug("Running flows from flow file.")

        self.db_server = self.get_connection()
        now = self.db_server.get_now()
        
        logging.info("Iterating over flows.")
        for flow in self.flows["flows"]:
            logging.info("Running {} flow.".format(flow["name"]))

            file = '{}/{}'.format(bucket, key)
            flow_type = flow["type"]
            flow['now'] = now
            logging.debug("Flow of type : {}".format(flow_type))

            if flow_type == "file_to_rv":

                    file_new_path = ""
                    try:
                        match = re.search(flow["file_regex"], file.name)
                    except AttributeError:
                        logging.error("Incorrect format for the filename: {}".format(file))
                    
                    try:
                        file_new_path = self.FileHandler.Move_To_Directory(bucket, 'work', key)                 
                        try:
                            self.db_server.copy_from(self.config["db_info"]["table_name"].format_map(flow), file_new_path, self.get_secret()['redshift'])
                                
                        except DBServerError.DataError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.DatabaseError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.OperationalError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.ProgrammingError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.InternalError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.IntegrityError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except DBServerError.DBError as err:
                            logging.error("Error type : " + type(err).__name__, err.args)  
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        except Exception as err:
                            logging.error("Error type : " + type(err).__name__, err.args)
                            self.FileHandler.Move_To_Directory(bucket, 'error', file_new_path)

                        else:
                            self.FileHandler.Move_To_Directory(bucket, 'done', file_new_path)
    
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
                        sql_bucket = self.config['data_directory_path']['config']['bucket']
                        sql_folder = self.config['data_directory_path']['config']['directories']['sql_scripts']

                        sql_script_file = self.FileHandler.get_file(sql_bucket, '{}/{}'.format(sql_folder, sql_script))
                        sql_script_path = '{}/{}/{}'.format(sql_bucket, sql_folder, sql_script)
                        
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
    def get_connection(self, ):

        db_server = None
        secret = self.get_secret()
                
        try:
            db_server = PostgreDBServer(secret)
        except DBServerError.OperationalError as err:
            logging.error("Error type : " + type(err).__name__, err.args)  
            raise DBServerError.OperationalError("Could not establish connection, {}".format(err.args))
            
        except Exception as err:
            logging.error("Connection error on {} with {} error.".format(self, err))
            print('Wrong username and password combination.')

        return db_server

    @logging_decorator
    def get_secret(self, ):
        secret_name = self.config['execution_environment']['aws']['db_info']['secret_name']
        

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name= 'secretsmanager',
            region_name= self.region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = str(base64.b64decode(get_secret_value_response['SecretBinary']))
            return secret