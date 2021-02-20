from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            # for line in f:      # GK: makes no sense, as input_file is in JSON
            data = json.load(f)   # read the whole file into data
            for line in data:     # loop over all lines
                print("*** PRS-line:", line)  # for debugging         
                message = self.dict_to_binary(line)
                # TODO send the correct data
                self.send(self.topic, message)  # send into Kafka topic
                # sleep time: default = 1 sec, I set to 0.01 to fill the Kafka topic much faster
                # The police jason file has ~200k records. Sending a record into Kafka at rate 1/0.01
                #     reads the json file completely in 34 min. Sending at rate 1.0 requires 55 hours
                time.sleep(0.01) # to watch screen output, set to 1.0, for experiments, use 0.01

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        ret = json.dumps(json_dict).encode('utf-8') # turn dict into JSON-formatted byte string
        return ret
        