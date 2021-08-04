#!/usr/bin/python
#-*- coding: utf-8 -*-

from DBServer import DBServer
import sys, psycopg2, logging, DBServerError, base64, boto3
from botocore.exceptions import ClientError
from Decorator import logging_decorator

class RedshiftDBServer(DBServer):
    __database_type__ = 'Redshift'

    @logging_decorator
    def __init__(self, _config):
        super.__init__(_config)
        self.conn = None
        secret = self._get_secret()
        self.db_name = secret['dbname']
        self.user = secret['username']
        self.port = secret['port']
        self.iam_role = secret['redshift_role']

        try:
            self.conn = psycopg2.connect(
                host= self._get_endpoint(secret['dbClusterIdentifier']),
                dbname= self.db_name,
                user= self.user,
                password= secret['password'],
                port= self.port
            )
            self.conn.autocommit = True
            logging.info("Established connection with the {} database with autocommit.".format(self.config['db_info']['engine']))

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            self.conn = None
            raise(err)

    #Execute an SQL query on the server instance.
    @logging_decorator
    def execSQL(self, query) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            logging.info("Executed query on {} type, {} database.".format(type(self), self.db_name))

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except Exception as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise Exception(' '.join(err.pgerror))

    #Bulk insert csv file at data_path into db_server instance in table_name
    @logging_decorator
    def copy_from(self, table_name, data_path) -> None:
        cur = self.conn.cursor()
        SQL_STATEMENT = "COPY {} FROM {} WITH CSV HEADER DELIMITER AS ',' iam-role '{}'"

        try:
            cur.execSQL(SQL_STATEMENT.format(table_name, data_path, self.iam_role))
            logging.info("{} loaded in {}. {} using {} database.".format(data_path, self.db_name, table_name, self.__database_type__))

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except FileNotFoundError as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on {}".format(data_path))
            self.conn.rollback()
            raise DBServerError.DBError(' '.join(err.args))

        except PermissionError as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on {}".format(data_path))
            self.conn.rollback()
            raise DBServerError.DBError(' '.join(err.args))
        
        except Exception as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.DBError(' '.join(err.args))
    
    @logging_decorator
    def get_now(self, ) -> str:
        now = ""
        logging.info("Fetching current time from database.")
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT TO_CHAR(now(),'YYYY-MM-DD HH24:MI:SS')")
                now = cur.fetchall()[0][0]
            logging.info("Current time is {}".format(now))

        except psycopg2.DatabaseError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))
        
        except Exception as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise Exception(' '.join(err.args))
        
        return now

    @logging_decorator
    def _get_secret(self, ):
        secret_name = self.config['db_info']['secret_name']
        

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name= 'secretsmanager',
            region_name= self.config['region']
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
    
    @logging_decorator
    def _get_endpoint(self, cluster_identifier):
        session = boto3.session.Session()
        client = session.client(
            service_name= 'redshift',
            region_name= self.config['region']
        )

        response = client.describe_clusters(
            ClusterIdentifier = cluster_identifier
        )['Clusters'][0]['ClusterNodes']

        for node in response:
            if node['NodeRole'] in ('SHARED', 'LEADER'):
                return node['PrivateIPAddress']

    #Closing the connection to the database
    @logging_decorator
    def closeConn(self, ):
        self.conn.close()
        
    # define a function that handles and parses psycopg2 exceptions
    @logging_decorator
    @staticmethod
    def _log_psycopg2_exception(err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno
        prompt_err = "Psycopg2 ERROR: ", err, "on line number: ", line_num, " traceback: ", traceback, "-- type: ", err_type, " extensions.Diagnostics: ", err.diag, " pgerror: ", err.pgerror, " pgcode: ", err.pgcode, "value: ", err_obj
        return prompt_err



