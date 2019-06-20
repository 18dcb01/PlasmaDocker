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
    id_ = 1000000000
    keep = []
    while True:
        if not client.contains(makeID("claims on " + str(id_))):
            client.put(1,makeID("claims on " + str(id_)))
            keep.append(client.get_buffers([makeID("claims on "+str(id_))]))

            [data] = client.get_buffers([makeID("dataset id"+str(id_))])

            client.put(1,makeID("loaded id " + str(id_)))
            keep.append(client.get_buffers([makeID("loaded id "+str(id_))]))


            buffer_ = pyarrow.BufferReader(data)
            reader = pyarrow.RecordBatchStreamReader(buffer_)

            dataTable = reader.read_all()
            #dataTable = dataTable.from_pandas(dataTable.to_pandas())
            exec(client.get(makeID("executable" + str(id_))))#execute

            reader = 0
            buffer_ = 0
            data = 0
            
            batches = dataTable.to_batches()

            strId = makeID("returnable"+str(id_))

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



            # newBatch = pyarrow.RecordBatch.from_pandas(pandas_)#convert back
            #
            # mock_sink = pyarrow.MockOutputStream()#find data size
            # stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, newBatch.schema)
            # stream_writer.write_batch(newBatch)
            # stream_writer.close()
            # data_size = mock_sink.size()
            #
            # buf = client.create(makeID("returnable"+str(id_)), data_size)
            #
            # stream = pyarrow.FixedSizeBufferWriter(buf)
            # stream_writer = pyarrow.RecordBatchStreamWriter(stream, newBatch.schema)
            # stream_writer.write_batch(newBatch)
            # stream_writer.close()
            #
            # client.seal(makeID("returnable"+str(id_)))

        else:
            id_+=1
