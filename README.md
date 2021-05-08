# API Gatekeeper

This repository will transport the data in real time API hit and then put it in responsible table in [Google Cloud Big Query](https://cloud.google.com/).

## Installation

Use git to clone this repository

```bash
git clone https://github.com/ghazimuharam/bq-api-gatekeeper
```

## Prerequisite

Make sure you have python 3.7 installed on your machine

```bash
> python --version
Python 3.7.10
```

To run the script in this repository, you need to install the prerequisite library from requirements.txt

```bash
pip install -r requirements.txt
```

Store all your service account json files to ./service directory

To run the script, you need to run kafka server on your machine, see [Kafka Docs](https://kafka.apache.org/documentation/#quickstart) for the installation.

create `api-gatekeeper` topic on you kafka server.

![kafka-topic](https://user-images.githubusercontent.com/22569688/117526112-1dfa8780-afed-11eb-88bc-da7798f3e6b1.png)

Before running the `setup.sh` script, you have to specify your Google Cloud Application Credentials using command below

```bash
export GOOGLE_APPLICATION_CREDENTIALS="./service/your-credentials.json"
```

### Main

Create dataset in bigquery using command below

```bash
sh setup.sh
```

![setup.sh](https://user-images.githubusercontent.com/22569688/117526734-2e146600-aff1-11eb-9231-2f61f93a1ae0.png)

Run main application for the API Endpoint using command below

```bash
uvicorn main:app --reload
```

![uvicorn](https://user-images.githubusercontent.com/22569688/117526647-d0801980-aff0-11eb-8692-8fd72a47bfd9.png)

Run Kafka Consumer script using command below

```bash
python kafka-consumer.py
```

![kafka-consumer.py](https://user-images.githubusercontent.com/22569688/117526883-302af480-aff2-11eb-9d0c-edc1feff1a5a.png)

## License

[MIT](https://choosealicense.com/licenses/mit/)
