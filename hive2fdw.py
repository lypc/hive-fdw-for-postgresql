from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

from pyhive import hive

class HiveServer2ForeignDataWrapper(ForeignDataWrapper):
    """
    Hive Server2 FDW for PostgreSQL
    """
    
    def __init__(self, options, columns):
        super(HiveServer2ForeignDataWrapper, self).__init__(options, columns)
        if 'host' not in options:
            log_to_postgres('The host parameter is required and the default is localhost.', WARNING)
        self.host = options.get("host", "localhost")
        
        if 'port' not in options:
            log_to_postgres('The host parameter is required and the default is 10000.', WARNING)
        self.port = options.get("port", "10000")
        
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
            client = hive.connect(self.host ,username='deploy', port=self.port)

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
    client = hive.connect('sha2dw02',username='deploy', port=26162)
    cursor = client.cursor()
    cursor.execute('select * from default.dual') 
    for row in cursor.fetchall():
        line = {}
        print row[0]
