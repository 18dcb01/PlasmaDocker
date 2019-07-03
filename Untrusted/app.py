import pyarrow
import pyarrow.plasma as plasma
import os
import time
import grpc
import codeRunner_pb2_grpc
import codeRunner_pb2
import random
import sys
import signal
from concurrent import futures


def demote():
    os.setgid(65534)
    os.setuid(65534)


def makeID(id_):
    return plasma.ObjectID(id_.encode("utf8"))


def randString():
    chars = """abcdefghijklmnopqr stuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()~`-_=+[{]};:'"/?>.<,|\\"""
    return ''.join([random.choice(chars) for n in range(20)])


runServer = True


def handler(signum, frame):
    runServer = False
    raise Exception("Timed out!")



class codeRunnerServicer(codeRunner_pb2_grpc.codeRunnerServicer):
    def __init__(self):
        self.client = plasma.connect("/tmp/plasma")
        demote()


    def runCode(self, request, context):
        start = time.clock()
        print(-1)
        print(0)

        [data] = self.client.get_buffers([makeID(request.id_)])
        buffer_ = pyarrow.BufferReader(data)
        reader = pyarrow.RecordBatchStreamReader(buffer_)
        dataTable = reader.read_all()
        
        signal.alarm(1)

        Vars = {'dataTable':dataTable}
        exec(request.toRun,globals(),Vars)#untrusted function
        signal.alarm(0)

        dataTable = Vars['newTable']

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
    signal.signal(signal.SIGALRM, handler)
    try:
        while(runServer):
            time.sleep(3)
    except:
        print("server stopped")
    finally:
        server.stop(0)
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    for i in pids:
        os.system("kill -9 "+i)#a terrible way to stop infinite loops
    
if __name__ == "__main__":
    serve()
