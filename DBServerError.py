class DBError(Exception):
    """Base class for exceptions in this module."""
    def __init__(self, message):
        self.message = message

class DatabaseError(DBError):
    """Exception raised for errors that are related to the database.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)

class OperationalError(DBError):
    """Exception raised for errors that are related to the database’s operation and not necessarily under the control of the programmer, 
    e.g. an unexpected disconnect occurs, the data source name is not found, 
    a transaction could not be processed, a memory allocation error occurred during processing, etc.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)

class DataError(DBError):
    """Exception raised for errors that are due to problems with the processed data like division by zero, numeric value out of range, etc

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)

class IntegrityError(DBError):
    """Exception raised when the relational integrity of the database is affected, e.g. a foreign key check fails.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)

class InternalError(DBError):
    """Exception raised when the database encounters an internal error, 
    e.g. the cursor is not valid anymore, the transaction is out of sync, etc.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)

class ProgrammingError(DBError):
    """Exception raised for programming errors, e.g. table not found or already exists, 
    syntax error in the SQL statement, wrong number of parameters specified, etc.

    Attributes:
        message -- explanation of the error
    """
    def __init__(self, message):
        super().__init__(message)