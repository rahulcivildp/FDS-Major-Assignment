# transaction_log.py

import json
import os

class TransactionLog:

    def __init__(self, node_id):
        self.file_name = os.path.join(
            "ledgers",
            f"ledger_{node_id}.json"
        )

        if not os.path.exists(self.file_name):
            with open(self.file_name, "w") as f:
                json.dump([], f)

    def append(self, transaction):

        with open(self.file_name, "r") as f:
            data = json.load(f)

        data.append(transaction)

        with open(self.file_name, "w") as f:
            json.dump(data, f, indent=4)

    def read(self):
        with open(self.file_name, "r") as f:
            return json.load(f)