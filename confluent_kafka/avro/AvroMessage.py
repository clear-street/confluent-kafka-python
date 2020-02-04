

class AvroDatum(object):
    __slots__ = ['schema', 'data']

    def __init__(self, schema, data):
        if schema is None:
            raise ValueError("Schema must be set")

        self.schema = schema
        self.data = data
