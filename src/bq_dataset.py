from google.cloud import bigquery
import sys


class BQDataset():
    def __init__(self, dataset) -> None:
        self.client = bigquery.Client()
        self.dataset_id = dataset
        self.create_dataset()

    def create_dataset(self):
        dataset = "{}.{}".format(self.client.project, self.dataset_id)

        dataset = bigquery.Dataset(dataset)
        dataset.location = "ASIA-SOUTHEAST2"

        # Make an API request.
        dataset = self.client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(
            self.client.project, dataset.dataset_id))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        BQDataset(sys.argv[1])
