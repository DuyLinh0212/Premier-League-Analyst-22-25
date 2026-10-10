"""Author-side export of tested Docker images without an intermediate large tar."""
import gzip
import hashlib
import json
import shutil
import subprocess
import time
from pathlib import Path

IMAGES = ['football-runtime:3.0','football-airflow:3.0','football-api:3.0','football-web:3.0',
          'apache/kafka:4.3.1','neo4j:5.26-community','postgres:16']

def main():
    destination=Path(__file__).resolve().parents[1]/'delivery'
    destination.mkdir(exist_ok=True)
    target=destination/'football-images.tar.gz'
    staging=destination/'football-images.tar.gz.part'
    if shutil.disk_usage(destination).free<8*1024**3:
        raise RuntimeError('Need at least 8 GiB free for image export')
    lock=[]
    for tag in IMAGES:
        item=json.loads(subprocess.check_output(['docker','image','inspect',tag]))[0]
        lock.append({'tag':tag,'id':item['Id'],'repo_digests':item.get('RepoDigests',[]),'architecture':item['Architecture'],'os':item['Os']})
    with staging.open('wb') as raw:
        with gzip.GzipFile(filename='',mode='wb',fileobj=raw,compresslevel=1,mtime=0) as compressed:
            process=subprocess.Popen(['docker','save',*IMAGES],stdout=subprocess.PIPE)
            try:
                last_report=time.monotonic()
                for chunk in iter(lambda:process.stdout.read(1024*1024),b''):
                    compressed.write(chunk)
                    if time.monotonic()-last_report>=20:
                        compressed.flush()
                        free=shutil.disk_usage(destination).free
                        print(f'Exported {raw.tell()/1024**3:.2f} GiB; disk free {free/1024**3:.2f} GiB',flush=True)
                        if free<512*1024**2:
                            raise RuntimeError('Image export stopped to preserve free disk space')
                        last_report=time.monotonic()
            except BaseException:
                process.terminate()
                raise
            finally:
                process.stdout.close()
            if process.wait()!=0:
                raise RuntimeError('docker save failed; export is incomplete')
    staging.replace(target)
    digest=hashlib.sha256()
    with target.open('rb') as source:
        for chunk in iter(lambda:source.read(1024*1024),b''):
            digest.update(chunk)
    result={'file':target.name,'bytes':target.stat().st_size,'sha256':digest.hexdigest(),'images':lock}
    (destination/'images_manifest.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({'bytes':result['bytes'],'sha256':result['sha256']}))

if __name__=='__main__':
    main()
