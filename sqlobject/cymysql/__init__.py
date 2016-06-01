from sqlobject.dbconnection import registerConnection


def builder():
    from . import cymysqlconnection
    return cymysqlconnection.CyMySQLConnection

registerConnection(['cymysql'], builder)
