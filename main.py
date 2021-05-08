import json
from typing import List, Union
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')


class Activity(BaseModel):
    operation: str
    table: str
    col_names: List[str]
    col_types: List[str]
    col_values: List


class Payload(BaseModel):
    activities: List[
        Activity
    ]


app = FastAPI()


@app.get("/")
def read_root():
    return {"status": 200}


@app.post("/api/activities")
def read_item(payload: Payload):
    json_compatible_item_data = jsonable_encoder(payload)

    producer.send('api-gatekeeper',
                  json.dumps(json_compatible_item_data).encode('utf-8'))

    return {"message": "ok", "payload": json_compatible_item_data}
