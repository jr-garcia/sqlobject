from sqlobject.mysql.mysqlconnection import *

import cymysql, cymysql.constants.CLIENT, cymysql.constants.ER
from cymysql.connections import Connection


class CyMySQLConnection(MySQLConnection):
    def __init__(self, db, user, password='', host='localhost', port=0, **kw):
        self.module = cymysql
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.kw = {}
        for key in ("unix_socket", "init_command",
                    "read_default_file", "read_default_group", "conv"):
            if key in kw:
                self.kw[key] = kw.pop(key)
        for key in ("connect_timeout", "compress", "named_pipe", "use_unicode",
                    "client_flag", "local_infile"):
            if key in kw:
                self.kw[key] = int(kw.pop(key))
        for key in ("ssl_key", "ssl_cert", "ssl_ca", "ssl_capath"):
            if key in kw:
                if "ssl" not in self.kw:
                    self.kw["ssl"] = {}
                self.kw["ssl"][key[4:]] = kw.pop(key)
        if "charset" in kw:
            self.dbEncoding = self.kw["charset"] = kw.pop("charset")
        else:
            self.dbEncoding = None

        global mysql_Bin
        if not PY2 and mysql_Bin is None:
            mysql_Bin = cymysql.Binary
            cymysql.Binary = lambda x: mysql_Bin(x).decode(
                'ascii', errors='surrogateescape')

        self._server_version = None
        self._can_use_microseconds = None
        DBAPI.__init__(self, **kw)

    def makeConnection(self):
        dbEncoding = self.dbEncoding
        if dbEncoding:
            if not hasattr(Connection, 'set_character_set'):
                # monkeypatch pre cymysql 1.2.1
                def character_set_name():
                    return dbEncoding + '_' + dbEncoding

                Connection.character_set_name = character_set_name
        try:
            kw = self.kw
            kw.update({'host': self.host, 'db': self.db,
                        'user': self.user, 'passwd': self.password})
            if self.port != 0:
                kw['port'] = self.port
            conn = self.module.connect(**kw)
            conn.ping(True)
        except self.module.OperationalError as e:
            conninfo = ("; used connection string: "
                        "host=%(host)s, port=%(port)s, "
                        "db=%(db)s, user=%(user)s" % self.__dict__)
            raise dberrors.OperationalError(ErrorMessage(e, conninfo))

        if hasattr(conn, 'autocommit'):
            conn.autocommit(bool(self.autoCommit))

        if dbEncoding:
            if hasattr(conn, 'set_character_set'):  # cymysql 1.2.1 and later
                conn.set_character_set(dbEncoding)
            else:
                conn.query("SET NAMES %s" % dbEncoding)

        return conn
