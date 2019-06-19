from flask import Flask
from redis import Redis, RedisError
import pyarrow
import pyarrow.plasma as plasma
import pyarrow.parquet as pq
import pandas
import os
import socket
import time

# Connect to Redis
redis = Redis(host="redis", db=0, socket_connect_timeout=2, socket_timeout=2)

app = Flask(__name__)

@app.route("/")
def hello():
    client = plasma.connect("/tmp/plasma")

    id_ = 1000000000
    while client.contains(plasma.ObjectID("claims on "+str(id_))):
        id_ += 1
    id_ -= 1 #Remove after implementing post-grab claims

    from pyarrow import csv
    fn = "mimic.csv"
    table = csv.read_csv(fn)
    batches = table.to_batches()

    strId = plasma.ObjectID("dataset id"+str(id_))

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

    code = "pandas_.iat[4,3] = 32048238"

    client.put(code, plasma.ObjectID("executable"+str(id_)))


    [data] = client.get_buffers([plasma.ObjectID("returnable"+str(id_))])

    buffer_ = pyarrow.BufferReader(data)
    reader = pyarrow.RecordBatchStreamReader(buffer_)
    record_batch = reader.read_next_batch()

    html = str(record_batch.to_pandas().mean())

    return html


if __name__ == "__main__":
    newpid = os.fork()

    if not newpid:#one path runs the plasma stores
        import subprocess
        subprocess.call(["plasma_store", "-m", "30000000", "-s", "/tmp/plasma"])

    else:
        app.run(host='0.0.0.0', port=80)
