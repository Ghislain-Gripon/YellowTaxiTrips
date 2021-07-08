#!/usr/bin/python
#-*- coding: utf-8 -*-

from DBServer import DBServer
import sys, psycopg2, logging, DBServerError
from Decorator import logging_decorator

class PostgreDBServer(DBServer):

    @logging_decorator
    def __init__(self, _config, _password, _user):
        super().__init__(_config, _user)
        self.conn = None

        try:
            self.conn = psycopg2.connect(
                host=self.endpoint,
                dbname=self.db_name,
                user=self.db_user,
                password=_password,
                port=self.port)
            self.conn.autocommit = True
            logging.info("Established connection with the database of PostgreDBServer type with autocommit.")

        except psycopg2.OperationalError as err:
            logging.error(self.log_psycopg2_exception(err))
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
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except Exception as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on query : {}".format(query))
            self.conn.rollback()
            raise Exception(' '.join(err.pgerror))

    #Bulk insert csv file at data_path into db_server instance in table_name
    @logging_decorator
    def copy_from(self, table_name: str, data_path, extension) -> None:

        cur = self.conn.cursor()
        options = {
            "txt": "TEXT",
            "csv": "CSV HEADER DELIMITER AS ','"
        }
        SQL_STATEMENT = "COPY {} FROM STDIN WITH {}"

        try:
            with open(data_path, 'r', encoding='ascii', errors='ignore') as f: #file stream on data_path for the STDIN canal of the COPY FROM postgre command
                cur.copy_expert(sql = SQL_STATEMENT.format(table_name, options[extension]), file=f)
            logging.info("{} loaded in {}. {} using {} database.".format(data_path, self.db_name, table_name, type(self).__name__))

        except psycopg2.OperationalError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.OperationalError(' '.join(err.pgerror))
        
        except psycopg2.DataError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.DataError(' '.join(err.pgerror))

        except psycopg2.IntegrityError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.IntegrityError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))

        except psycopg2.ProgrammingError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("Error on copy from {} to {}".format(table_name, data_path))
            self.conn.rollback()
            raise DBServerError.ProgrammingError(' '.join(err.pgerror))

        except psycopg2.DatabaseError as err:
            logging.error(self.log_psycopg2_exception(err))
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
            logging.error(self.log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise DBServerError.DatabaseError(' '.join(err.pgerror))

        except psycopg2.InternalError as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise DBServerError.InternalError(' '.join(err.pgerror))
        
        except Exception as err:
            logging.error(self.log_psycopg2_exception(err))
            logging.error("An error happened while retrieving the current time from the database. {}".format(err))
            self.conn.rollback()
            raise Exception(' '.join(err.args))
        
        return now

    #Closing the connection to the database
    @logging_decorator
    def closeConn(self, ) -> None:
        self.conn.close()
        
    # define a function that handles and parses psycopg2 exceptions
    @logging_decorator
    @staticmethod
    def log_psycopg2_exception(err) -> str:
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno
        prompt_err = "Psycopg2 ERROR: ", err, "on line number: ", line_num, " traceback: ", traceback, "-- type: ", err_type, " extensions.Diagnostics: ", err.diag, " pgerror: ", err.pgerror, " pgcode: ", err.pgcode, "value: ", err_obj
        return prompt_err



