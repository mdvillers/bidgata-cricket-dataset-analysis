from pyhdfs_client.pyhdfs_client import HDFSClient
import os

def upload(datapath):

    csv_files = [f for f in os.listdir(datapath) if f.endswith('.csv')]
    print(csv_files)

    hdfs_client = HDFSClient()

    _,out,_ = hdfs_client.run(['-ls'])

    if " cricket_dataset " in out:
      hdfs_client.run(['-rm','-R','cricket_dataset'])

    hdfs_client.run(['-mkdir','cricket_dataset'])

    for i in csv_files:
      print(i)
      hdfs_client.run(['-copyFromLocal', f'{datapath}/{i}','cricket_dataset'])

    ret, out, err = hdfs_client.run(['-ls', 'cricket_dataset'])
    print(out)

    return  hdfs_client.stop()
