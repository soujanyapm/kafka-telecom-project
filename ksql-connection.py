#from ksql import KSQLAPI
# client = KSQLAPI('http://localhost:8088')

# client.ksql('show tables')
import requests
import json

from kafka import KafkaConsumer
consumer = KafkaConsumer('users')
for message in consumer:
    print ((message.key).decode("utf-8") )
    user = (message.key).decode("utf-8")
    user_id = int(user.split("_")[1])

    #print(json.loads((message.value).decode("utf-8")))
    if (user_id <= 5):
        URL = "https://hooks.slack.com/services/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"
        body = {"text":"Hello "+user}
        headers = {"Content-Type":"application/json"}

        result = requests.post(URL, data = json.dumps(body),headers=headers)

        print(result)
