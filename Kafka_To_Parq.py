from confluent_kafka import Consumer, KafkaException
import json
import logging
import multiprocessing
from datetime import datetime, timedelta
import pandas as pd
import sys

class Kafka_to_Parq(multiprocessing.Process):

  def __init__(self, pid, kafka_broker, topic, group, dt_start, dt_end):
    multiprocessing.Process.__init__(self)
    self.kafka_broker = kafka_broker
    self.topic = topic
    self.group = group
    self.pid = pid
    self.dt_start = dt_start
    self.dt_end = dt_end

  def run(self):

    conf = {'bootstrap.servers': self.kafka_broker,
            'group.id': self.group,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'
            }

    logger = logging.getLogger('consumer_' + str(self.pid))
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)
    logger.info("Config: " + str(conf))
    logger.info("Creating Consumer")
    c = Consumer(conf, logger=logger)

    def print_assignment(consumer, partitions):
      print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(self.topic, on_assign=print_assignment)

    l_val = []
    fcount = 0
    msg_count = 0
    pq_id = 0
    pq_limit = 100000
    # Read messages from Kafka
    while True:
      msg = c.poll(timeout=1.0)

      if msg is None:
        continue

      if msg.error():
        raise KafkaException(msg.error())
      else:
        # Proper message
        msg_dict = json.loads(msg)
        timestamp = msg_dict["@timestamp"]
        dt_ts = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        if dt_ts >= self.dt_end:
          break
        elif dt_ts < self.dt_start:
          continue

        host = msg_dict["host"]["name"]
        kmsg = msg_dict["message"]
        l_val.append((host, dt_ts, kmsg))
        fcount += 1
        if pq_limit == fcount:
          pq_id += 1
          pq_fn = "./" + self.topic + "_" + str(self.pid) + "_" + str(pq_id) + ".parquet"
          df = pd.DataFrame(l_val, columns=["host", "timestamp", "message"])
          logger.info("Writing Parq file: " + pq_fn)
          df.to_parquet(pq_fn, compression='snappy')
          logger.info("Written count: " + str(fcount))
          msg_count += fcount
          logger.info("Processed count: " + str(msg_count))
          fcount = 0
          l_val = []

    if fcount > 0:
      pq_id += 1
      pq_fn = "./" + self.topic + "_" + str(self.pid) + "_" + str(pq_id)
      df = pd.DataFrame(l_val, columns=["host", "timestamp", "message"])
      logger.info("Writing Parq file: " + pq_fn)
      df.to_parquet(pq_fn, compression='snappy')
      logger.info("Written count: " + str(fcount))
      msg_count += fcount
      logger.info("Total Processed count: " + str(msg_count))

    c.close()

if __name__ == '__main__':
  if len(sys.argv) < 2:
    print("Usage: python Kafka_To_Parq.py <kafka_broker> <topic> <group> <date start[yyyy-mm-dd]> <date_end[yyyy-mm-dd]>")
    sys.exit(1)

  kafka_broker = sys.argv[1]
  topic = sys.argv[2]
  group = sys.argv[3]
  dstr_start = sys.argv[4]
  dstr_end = sys.argv[5]

  proc_lst = []
  dt_start = datetime.strptime(dstr_start, "%Y-%m-%d")
  dt_end = datetime.strptime(dstr_end, "%Y-%m-%d") + timedelta(days=1)

  for pid in range(1, 6):
    print("Starting Process " + str(pid))
    p = Kafka_to_Parq(pid, kafka_broker, topic, group, dt_start, dt_end)
    proc_lst.append(p)
    p.start()

  print("Waiting Processes...")
  for p in proc_lst:
    p.join()

  print("Completed")


