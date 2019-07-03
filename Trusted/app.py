from flask import Flask
from redis import Redis, RedisError
import pyarrow
import pyarrow.plasma as plasma
import pyarrow.parquet as pq
import os
import time
import random
import grpc
import codeRunner_pb2_grpc
import codeRunner_pb2

# Connect to Redis
redis = Redis(host="redis", db=0, socket_connect_timeout=2, socket_timeout=2)

app = Flask(__name__)


def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))


def randString():
    chars = """abcdefghijklmnopqr stuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()~`-_=+[{]};:'"/?>.<,|\\"""
    return ''.join([random.choice(chars) for n in range(20)])


@app.route("/")
def hello():
    channel = grpc.insecure_channel('untrusted:50051')
    stub = codeRunner_pb2_grpc.codeRunnerStub(channel)

    rand = random.choice([True,False])

    from pyarrow import csv
    fn = "IRAhandle_tweets_1.csv" if rand else "mimic.csv"
    table = csv.read_csv(fn)
    start = time.clock()

    print("data loaded")

    batches = table.to_batches()
    print(1)
    client = plasma.connect("/tmp/plasma")

    print(2)

    code = """
import time
while True:
    print(7)
    time.sleep(0.5)
""" if False else """
import os
import pyarrow
import sys

authors = dataTable.column("author")
newData = []
for i in range(len(authors)):
    newData.append(1 if i == 0 or authors[i] != authors[i-1] else newData[-1]+1)
newColumn = dataTable.column(3).from_array("authorTweetCount", [newData])
newTable = dataTable.append_column(newColumn)
    """ if rand else"""
import os
import pyarrow
import sys

ages = dataTable.column("age")
maxV = max(ages.to_pylist())
newData = []
for i in ages:
    newData.append(1 if i == maxV else 0)
newColumn = dataTable.column(3).from_array("oldest", [newData])
newTable = dataTable.append_column(newColumn)
    """

    tables = []

    for i in range(len(batches)):
        id_ = randString()

        strId = makeID(id_)
        
        mock_sink = pyarrow.MockOutputStream()  #find data size
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, batches[0].schema)
        stream_writer.write_batch(batches[i])
        stream_writer.close()
        data_size = mock_sink.size()
        buf = client.create(strId, data_size)

        stream = pyarrow.FixedSizeBufferWriter(buf)
        stream_writer = pyarrow.RecordBatchStreamWriter(stream, batches[0].schema)
        stream_writer.write_batch(batches[i])
        stream_writer.close()

        client.seal(strId)
        print("sent batch "+str(i+1))
        
        codeToSend = codeRunner_pb2.code(toRun=code,id_=id_)
        
        newId = stub.runCode(codeToSend,timeout=1)
        newId = newId.id_
        
        [data] = client.get_buffers([makeID(newId)])
        outputBuf = pyarrow.py_buffer(data.to_pybytes())
        buffer_ = pyarrow.BufferReader(outputBuf)
        reader = pyarrow.RecordBatchStreamReader(buffer_)
        if i == 0:
            datatable = reader.read_all()
        else:
            datatable = pyarrow.concat_tables([datatable,datatable.from_batches(reader.read_all().to_batches())])

    html = str(datatable.column("authorTweetCount" if rand else "oldest").data)
    print("data received after " +str(time.clock()-start))

    return html


if __name__ == "__main__":
    newpid = os.fork()
    print(1)

    if not newpid:  #one path runs the plasma stores
        import subprocess
        print(2)
        
        subprocess.call(["plasma_store", "-m", "60000000", "-s", "/tmp/plasma"])
        assert false  #plasma store stopped?

    else:
        client = plasma.connect("/tmp/plasma")
        
        app.run(host='0.0.0.0', port=80)
