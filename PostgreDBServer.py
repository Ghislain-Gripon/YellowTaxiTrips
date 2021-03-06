#!/usr/bin/python
#-*- coding: utf-8 -*-

from DBServer import DBServer
import sys
import psycopg2
import logging
import DBServerError
import pathlib
from Decorator import logging_decorator
from getpass import getpass

class PostgreDBServer(DBServer):

    @logging_decorator
    def __init__(self, _config):
        super().__init__(_config)
        self.conn = None

        logging.info("Requesting username and password of database.")
        _user, _password = self._request_info()

        try:
            self.conn = psycopg2.connect(
                host= self.config['db_info']['server'],
                dbname= self.config['db_info']['name'],
                user= _user,
                password= _password,
                port= self.config['db_info']['port'])
            self.conn.autocommit = True
            logging.info("Established connection with the database of PostgreDBServer type with autocommit.")

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error(err)
            self.conn = None
            raise(err)

    
    @staticmethod
    @logging_decorator
    def _request_info():
        logging.debug("Workflow.request_info starting.")
        username:str = input('Username: ')
        password:str = getpass()
        logging.debug("Workflow.request_info with username : {}".format(username))
        return username, password

    #Execute an SQL query on the server instance.
    @logging_decorator
    def execSQL(self, query:str) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            logging.info("Executed query on {} type, {} database.".format(type(self), self.db_name))

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except Exception as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            logging.error(err)
            self.conn.rollback()
            raise Exception(' '.join(err.pgerror))

    #Bulk insert csv file at data_path into db_server instance in table_name
    @logging_decorator
    def copy_from(self, table_name:str, data_path:str) -> None:

        cur = self.conn.cursor()
        options = {
            ".txt": "TEXT DELIMITER '|'",
            ".csv": "CSV HEADER DELIMITER ','"
        }
        SQL_STATEMENT = "COPY {} FROM STDIN WITH {}"

        try:
            with open(data_path, 'r', encoding='ascii', errors='ignore') as f: #file stream on data_path for the STDIN canal of the COPY FROM postgre command
                cur.copy_expert(sql = SQL_STATEMENT.format(table_name, options[pathlib.Path(data_path).suffix]), file=f)
            logging.info("{} loaded in {}. {} using {} database.".format(data_path, self.config['db_info']['name'], table_name, type(self).__name__))

        except psycopg2.OperationalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except FileNotFoundError as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on {}".format(data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DBError(' '.join(err.args))

        except PermissionError as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on {}".format(data_path))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DBError(' '.join(err.args))
        
        except Exception as err:
            logging.error("Error type : " + type(err).__name__)
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            logging.error(err)
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
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            logging.error(err)
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))
        
        except Exception as err:
            logging.error(self._log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            logging.error(err)
            self.conn.rollback()
            raise Exception(' '.join(err.args))
        
        return now

    #Closing the connection to the database
    @logging_decorator
    def closeConn(self, ) -> None:
        self.conn.close()
        
    # define a function that handles and parses psycopg2 exceptions
    @staticmethod
    @logging_decorator
    def _log_psycopg2_exception(err) -> str:
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno
        prompt_err = "Psycopg2 ERROR: ", err, "on line number: ", line_num, " traceback: ", traceback, "-- type: ", err_type, " extensions.Diagnostics: ", err.diag, " pgerror: ", err.pgerror, " pgcode: ", err.pgcode, "value: ", err_obj
        return prompt_err



