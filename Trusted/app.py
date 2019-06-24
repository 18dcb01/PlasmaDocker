from flask import Flask
from redis import Redis, RedisError
import pyarrow
import pyarrow.plasma as plasma
import pyarrow.parquet as pq
import os
import socket
import time

# Connect to Redis
redis = Redis(host="redis", db=0, socket_connect_timeout=2, socket_timeout=2)

app = Flask(__name__)

def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))

@app.route("/")
def hello():
    client = plasma.connect("/tmp/plasma")

    id_ = 1000000000
    while client.contains(makeID("loaded id "+str(id_))):
        id_ += 1

    from pyarrow import csv
    fn = "mimic.csv"
    table = csv.read_csv(fn)
    batches = table.to_batches()

    strId = makeID("dataset id"+str(id_))

    mock_sink = pyarrow.MockOutputStream()#find data size
    stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, batches[0].schema)
    for batch in batches:
        stream_writer.write_batch(batch)
    stream_writer.close()
    data_size = mock_sink.size()

    buf = client.create(strId, data_size)

    stream = pyarrow.FixedSizeBufferWriter(buf)
    stream_writer = pyarrow.RecordBatchStreamWriter(stream, batches[0].schema)
    for batch in batches:
        stream_writer.write_batch(batch)
    stream_writer.close()

    client.seal(strId)

    code = """
import os
import pyarrow
import sys
import pyarrow.plasma

reader = pyarrow.RecordBatchStreamReader(sys.stdin.buffer)

dataTable = reader.read_all()

maxV = max(dataTable.column("age").to_pylist())
newData = []
for i in dataTable.column("age").data:
    newData.append(1 if i == maxV else 0)
newColumn = dataTable.column(3).from_array("oldest", [newData])
dataTable = dataTable.append_column(newColumn)

batches = dataTable.to_batches()

stream_writer = pyarrow.RecordBatchStreamWriter(sys.stdout.buffer, dataTable.schema)
for batch in batches:
    stream_writer.write_batch(batch)
stream_writer.close()
    """

    client.put(code, makeID("executable"+str(id_)))


    [data] = client.get_buffers([makeID("returnable"+str(id_))])

    buffer_ = pyarrow.BufferReader(data)
    reader = pyarrow.RecordBatchStreamReader(buffer_)
    datatable = reader.read_all()

    html = str(datatable)
    return html


if __name__ == "__main__":
    newpid = os.fork()

    if not newpid:#one path runs the plasma stores
        import subprocess
        subprocess.call(["plasma_store", "-m", "60000000", "-s", "/tmp/plasma"])
        assert false #plasma store stopped?

    else:
        app.run(host='0.0.0.0', port=80)
        #hello()
