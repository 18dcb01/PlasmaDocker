



"""
import pyarrow
import pyarrow.plasma as plasma
import os
import socket
import time
import subprocess
import signal
import grpcio



def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))


def demote():
    os.setgid(65534)
    os.setuid(65534)

def handler(signum, frame):
    raise Exception("Timed out!")


if __name__ == "__main__":
    id_ = 10000
    keep = []
    client = plasma.connect("/tmp/plasma")
    while client.contains(makeID("untrusted claim" + str(id_))):
        id_ += 1
    client.put(1, makeID("untrusted claim" + str(id_)))
    keep = client.get_buffers([makeID("untrusted claim"+str(id_))])
    print(str(id_)+" claimed")

    numBuffers = client.get(makeID("trusted reserve" + str(id_)))

    signal.signal(signal.SIGALRM, handler)

    for i in range(numBuffers):
        
        start = time.clock()
        [data] = client.get_buffers([makeID("sent data "+str(id_)+str(i+10000))])

        print("data recieved")

        toExecute = ['python', '-c', client.get(makeID("user-made code " + str(id_)))]

        sub = subprocess.Popen(toExecute, stdin=subprocess.PIPE, stdout=subprocess.PIPE, preexec_fn=demote)
        print("sending data")
        
        signal.alarm(1)
        try:
            newData, _ = sub.communicate(data)
        except:
            newData = data
            print("request timed out")
        finally:
            signal.alarm(0)
        print("process finished")

        buffer_ = pyarrow.BufferReader(newData)
        reader = pyarrow.RecordBatchStreamReader(buffer_)

        dataTable = reader.read_all()
        print("data recieved")

        batches = dataTable.to_batches()

        strId = makeID("to return "+str(id_)+str(i+10000))

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, batches[0].schema)
        for batch in batches:
            stream_writer.write_batch(batch)
        stream_writer.close()
        data_size = mock_sink.size()

        buf = client.create(strId, data_size)

        stream = pyarrow.FixedSizeBufferWriter(buf)
        stream_writer = pyarrow.RecordBatchStreamWriter(stream, batches[0].schema)
        for j, batch in enumerate(batches):
            print("returning " + str(j+1) + "/" +str(len(batches)))
            stream_writer.write_batch(batch)
        stream_writer.close()

        buf = 0

        client.seal(strId)
        print(str(i+1)+"/"+str(numBuffers) + " in " +str(time.clock()-start))
"""
import pyarrow
import pyarrow.plasma as plasma
import os
import socket
import time
import subprocess
import signal
import grpc
import codeRunner_pb2_grpc
import codeRunner_pb2
import random
from concurrent import futures



def demote():
    os.setgid(65534)
    os.setuid(65534)

def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))

def randString():
    chars = """abcdefghijklmnopqr stuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()~`-_=+[{]};:'"/?>.<,|\\"""
    return ''.join([random.choice(chars) for n in range(20)])


class codeRunnerServicer(codeRunner_pb2_grpc.codeRunnerServicer):
    def __init__(self):
        self.client = plasma.connect("/tmp/plasma")
        demote()


    def runCode(self, request, context):
        start = time.clock()

        [data] = self.client.get_buffers([makeID(request.id_)])
        buffer_ = pyarrow.BufferReader(data)
        reader = pyarrow.RecordBatchStreamReader(buffer_)
        dataTable = reader.read_all()
        
        Vars = {'dataTable':dataTable}
        exec(request.toRun,globals(),Vars)

        dataTable = Vars.get('newTable')

        batches = dataTable.to_batches()

        id_ = randString()

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, batches[0].schema)
        for batch in batches:
            stream_writer.write_batch(batch)
        stream_writer.close()
        data_size = mock_sink.size()

        buf = self.client.create(makeID(id_), data_size)

        stream = pyarrow.FixedSizeBufferWriter(buf)
        stream_writer = pyarrow.RecordBatchStreamWriter(stream, batches[0].schema)
        for j, batch in enumerate(batches):
            print("returning " + str(j+1) + "/" +str(len(batches)))
            stream_writer.write_batch(batch)
        stream_writer.close()

        self.client.seal(makeID(id_))
        print("time elapsed: " + str(time.clock()-start))
        return codeRunner_pb2.id_(id_=id_)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = 1))
    codeRunner_pb2_grpc.add_codeRunnerServicer_to_server(
    codeRunnerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    try:
        while(True):
            time.sleep(5)
    except:
        server.stop(0)
    
if __name__ == "__main__":
    serve()
