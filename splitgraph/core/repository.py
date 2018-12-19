import splitgraph as sg
from .image import Image


class Repository:
    """
    Shim, experimenting with Splitgraph OO API

    Currently calls into the real API functions but we'll obviously move them here if we go ahead with this.
    """
    def __init__(self, repository, engine=None):
        self.engine = engine or sg.get_engine()
        self.repository = sg.to_repository(repository)

    def checkout(self, image):
        sg.checkout(self.repository, image_hash=image)

    def uncheckout(self):
        sg.uncheckout(self.repository)

    def get_image(self, image_hash):
        image_tuple = sg.get_image(self.repository, image=image_hash)
        # hacky hacky hack
        result = Image(*list(image_tuple))
        # pass ourselves into the image -- this means that the image will have to do
        # self.repository.repository to get to the classic sg Repository object (namespace + repository)
        result.repository = self
        return result

    def get_images(self):
        return [img[0] for img in sg.get_all_image_info(self.repository)]

    def push(self, destination=None):
        if destination:
            sg.push(self.repository, remote_engine=destination.engine, remote_repository=destination.repository)
        else:
            sg.push(self.repository)

    def pull(self):
        sg.pull(self.repository)

    def get_head(self):
        return sg.get_current_head(self.repository)

    def get_upstream(self):
        return sg.get_upstream(self.repository)

    def set_upstream(self, remote_name, remote_repository):
        sg.set_upstream(self.repository, remote_name, remote_repository)

    def commit(self):
        return sg.commit(self.repository)

    def run_sql(self, sql):
        engine = self.engine
        engine.run_sql("SET search_path TO %s", (self.repository.to_schema(),))
        result = self.engine.run_sql(sql)
        engine.run_sql("SET search_path TO public")
        return result

    def diff(self, table_name, image_1, image_2=None, aggregate=True):
        return sg.diff(self.repository, table_name, image_1, image_2, aggregate)
