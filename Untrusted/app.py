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

if __name__ == "__main__":
    client = plasma.connect("/tmp/plasma")
    newpid = True
    id_ = 1000000000
    while True:
        if not client.contains(plasma.ObjectID("claims on " + str(id_))):
            client.put(1,plasma.ObjectID("claims on " + str(id_)))

            [data] = client.get_buffers([plasma.ObjectID("dataset id"+str(id_))])

            #todo: add post-grab claim

            buffer_ = pyarrow.BufferReader(data)
            reader = pyarrow.RecordBatchStreamReader(buffer_)
            record_batch = reader.read_next_batch()

            client._release(plasma.ObjectID("dataset id"+str(id_)))

            pandas_ = record_batch.to_pandas()#convert to pandas

            exec(client.get(plasma.ObjectID("executable" + str(id_))))#execute

            newBatch = pyarrow.RecordBatch.from_pandas(pandas_)#convert back

            mock_sink = pyarrow.MockOutputStream()#find data size
            stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, newBatch.schema)
            stream_writer.write_batch(newBatch)
            stream_writer.close()
            data_size = mock_sink.size()

            buf = client.create(plasma.ObjectID("returnable"+str(id_)), data_size)

            stream = pyarrow.FixedSizeBufferWriter(buf)
            stream_writer = pyarrow.RecordBatchStreamWriter(stream, newBatch.schema)
            stream_writer.write_batch(newBatch)
            stream_writer.close()

            client.seal(plasma.ObjectID("returnable"+str(id_)))
        else:
            id_+=1
