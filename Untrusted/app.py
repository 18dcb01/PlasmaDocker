import pyarrow
import pyarrow.plasma as plasma
import os
import socket
import time
import subprocess


def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))


def demote():
    os.setgid(65534)
    os.setuid(65534)


if __name__ == "__main__":
    id_ = 1000000000
    keep = []
    while True:
        client = plasma.connect("/tmp/plasma")
        if not client.contains(makeID("claims on " + str(id_))):
            client.put(1, makeID("claims on " + str(id_)))
            keep.append(client.get_buffers([makeID("claims on "+str(id_))]))

            [data] = client.get_buffers([makeID("dataset id"+str(id_))])

            client.put(1, makeID("loaded id " + str(id_)))
            keep.append(client.get_buffers([makeID("loaded id "+str(id_))]))

            toExecute = ['python', '-c', client.get(makeID("executable" + str(id_)))]

            sub = subprocess.Popen(toExecute, stdin=subprocess.PIPE, stdout=subprocess.PIPE, preexec_fn=demote)

            data, _ = sub.communicate(data)

            buffer_ = pyarrow.BufferReader(data)
            reader = pyarrow.RecordBatchStreamReader(buffer_)

            dataTable = reader.read_all()

            batches = dataTable.to_batches()

            strId = makeID("returnable"+str(id_))

            mock_sink = pyarrow.MockOutputStream()
            stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, batches[0].schema)
            for batch in batches:
                stream_writer.write_batch(batch)
            stream_writer.close()
            data_size = mock_sink.size()

            buf = client.create(strId, data_size)

            stream = pyarrow.FixedSizeBufferWriter(buf)
            stream_writer = pyarrow.RecordBatchStreamWriter(stream, batches[0].schema)
            for i, batch in enumerate(batches):
                stream_writer.write_batch(batch)
            stream_writer.close()

            client.seal(strId)
        else:
            id_ += 1
        client.disconnect()