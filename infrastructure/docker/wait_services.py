"""Readiness of existing infrastructure; no provisioning."""
import json,os,socket,time,urllib.request
from urllib.parse import urlparse
from analytics import neon_store

def main(timeout=180):
    deadline=time.monotonic()+timeout
    last=None
    hdfs=urlparse(os.environ['HDFS_URI'])
    endpoint=os.getenv('NAMENODE_HTTP_URI','http://'+hdfs.hostname+':9870').rstrip('/')
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen(endpoint+'/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo',timeout=5) as response:
                node=json.load(response)['beans'][0]
            if node['Safemode'] or len(json.loads(node['LiveNodes']))<int(os.getenv('HDFS_MIN_DATANODES','1')):
                raise RuntimeError('Existing HDFS is recovering or has too few live DataNodes')
            with socket.create_connection((hdfs.hostname,hdfs.port or 9000),timeout=5):
                pass
            host,port=os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')[0].rsplit(':',1)
            with socket.create_connection((host,int(port)),timeout=5):
                pass
            neon_store.schema()
            with neon_store._connect() as connection, connection.cursor() as cursor:
                cursor.execute('SELECT 1')
                cursor.fetchone()
            print('Existing HDFS, Kafka and Neon PostgreSQL ready',flush=True)
            return
        except Exception as error:
            last=error
            time.sleep(5)
    raise RuntimeError('External services unavailable: '+str(last)) from last

if __name__=='__main__':
    main()
