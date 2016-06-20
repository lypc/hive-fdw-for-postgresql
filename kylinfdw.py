from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

import pykylin

class KylinForeignDataWrapper(ForeignDataWrapper):
    """
    Hive Server2 FDW for PostgreSQL
    """
    
    def __init__(self, options, columns):
        super(KylinForeignDataWrapper, self).__init__(options, columns)
        if 'host' not in options:
            log_to_postgres('The host parameter is required and the default is localhost.', WARNING)
        self.host = options.get("host", "localhost")
        
        if 'port' not in options:
            log_to_postgres('The host parameter is required and the default is 10000.', WARNING)
        self.port = options.get("port", "10000")
        
        if 'user' not in options:
            log_to_postgres('The user parameter is required and the default is ADMIN.', WARNING)
        self.user = options.get("user", "ADMIN")
        
        if 'password' not in options:
            log_to_postgres('The password parameter is required and the default is KYLIN.', WARNING)
        self.password = options.get("password", "KYLIN")
        
        if 'project' not in options:
            log_to_postgres('The project parameter is required and the default is empty string.', WARNING)
        self.project = options.get("project", "")
        
        if 'limit' not in options:
            log_to_postgres('The limit parameter is required and the default is 50000.', WARNING)
        self.limit = options.get("limit", "50000")
        
        if 'table' and 'query' not in options:
            log_to_postgres('table or query parameter is required.', ERROR)
        self.table = options.get("table", None)
        self.query = options.get("query", None)
            
        self.columns = columns

    def execute(self, quals, columns):
        if self.query:
            statement = self.query
        else:
            statement = "SELECT " + ",".join(self.columns.keys()) + " FROM " + self.table
        
        log_to_postgres('Hive query: ' + unicode(statement), DEBUG)
        
        try:
            endpoint = 'http://{}:{}/kylin/api'.format(self.host, self.port)
            client = pykylin.connect(username=self.user, password=self.password, endpoint=endpoint, project=self.project, limit=self.limit)

            cursor = client.cursor()

            cursor.execute(statement) 
            
            for row in cursor.fetchall():
                line = {}
                idx = 0
                for column_name in self.columns:
                    line[column_name] = row[idx]
                    idx = idx + 1
                yield line
                
        except NotImplementedError, ix:
            log_to_postgres(ix.message, ERROR)
        except Exception, ex:
            log_to_postgres(ex.message, ERROR)
        finally:
            client.close()

if __name__ == '__main__':
    client = pykylin.connect(username='ADMIN', password='KYLIN', endpoint='http://sha2dw03:7070/kylin/api', project='LiuLiang')
    cursor = client.cursor()
    sql = """
    select
    TRANSACTION_DATE,
    count(1)
    from F_T_CONTACT 
    group by F_T_CONTACT.TRANSACTION_DATE 
    limit 10
    """
    cursor.execute(sql) 
    for row in cursor.fetchall():
        line = {}
        print row
