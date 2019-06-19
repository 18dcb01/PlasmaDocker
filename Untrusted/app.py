from flask import Flask
from redis import Redis, RedisError
import pyarrow
import pyarrow.plasma as plasma
import pandas
import os
import socket
import time

# Connect to Redis
redis = Redis(host="redis", db=0, socket_connect_timeout=2, socket_timeout=2)

app = Flask(__name__)

def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))

if __name__ == "__main__":
    client = plasma.connect("/tmp/plasma")
    newpid = True
    id_ = 1000000000
    while True:
        if not client.contains(makeID("claims on " + str(id_))):
            client.put(1,makeID("claims on " + str(id_)))

            [data] = client.get_buffers([makeID("dataset id"+str(id_))])

            client.put(1,makeID("loaded id " + str(id_)))
            buffer_ = pyarrow.BufferReader(data)
            reader = pyarrow.RecordBatchStreamReader(buffer_)
            record_batch = []

            for i in range(10):
                record_batch.append(reader.read_next_batch())



            pandas_ = record_batch[0].to_pandas()#convert to pandas

            exec(client.get(makeID("executable" + str(id_))))#execute

            record_batch = []
            reader = 0
            buffer_ = 0
            data = 0

            newBatch = pyarrow.RecordBatch.from_pandas(pandas_)#convert back

            mock_sink = pyarrow.MockOutputStream()#find data size
            stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, newBatch.schema)
            stream_writer.write_batch(newBatch)
            stream_writer.close()
            data_size = mock_sink.size()

            buf = client.create(makeID("returnable"+str(id_)), data_size)

            stream = pyarrow.FixedSizeBufferWriter(buf)
            stream_writer = pyarrow.RecordBatchStreamWriter(stream, newBatch.schema)
            stream_writer.write_batch(newBatch)
            stream_writer.close()

            client.seal(makeID("returnable"+str(id_)))
        else:
            id_+=1
