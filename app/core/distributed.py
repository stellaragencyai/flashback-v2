from redis import Redis
from rq import Queue

redis_conn = Redis(host="127.0.0.1", port=6379)
queue = Queue("flashback", connection=redis_conn)

def submit(job, *args):
    return queue.enqueue(job, *args)
