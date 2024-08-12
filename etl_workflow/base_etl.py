class BaseETL:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None

    def connect_to_db(self):
        raise NotImplementedError("Connect to DB method not implemented")

    def extract(self):
        raise NotImplementedError("Extract method not implemented")

    def transform(self):
        pass  # Optional, only if transformation is needed

    def load(self):
        raise NotImplementedError("Load method not implemented")

    def run(self):
        self.connect_to_db()
        self.extract()
        self.transform()
        self.load()
        if self.conn:
            self.conn.close()