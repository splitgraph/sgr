import splitgraph as sg


class Table:
    def __init__(self, repository, image, table_name, objects):
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.objects = objects

    # todo object API too