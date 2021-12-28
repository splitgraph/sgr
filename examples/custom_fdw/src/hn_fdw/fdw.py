import logging
from concurrent.futures.thread import ThreadPoolExecutor

import requests

try:
    from multicorn import ANY, ForeignDataWrapper
except ImportError:
    # Multicorn not installed (don't crash the import in this case if we want to unit test this.).
    ForeignDataWrapper = object
    ANY = object()


class HNForeignDataWrapper(ForeignDataWrapper):
    def get_rel_size(self, quals, columns):
        """
        Method called from the planner to estimate the resulting relation size for a scan.

        :returns Tuple of (number of rows, row width in bytes)
        """

        # The stories endpoints return a maximum of 500 rows.
        if self.endpoint in ["topstories", "newstories", "beststories"]:
            return 500, 1000

        # The Show/Ask HN endpoints return 200 rows
        return 200, 1000

    def _full_endpoint(self):
        return "https://hacker-news.firebaseio.com/v0/%s.json" % self.endpoint

    @staticmethod
    def _story_endpoint(story_id):
        return "https://hacker-news.firebaseio.com/v0/item/%d.json" % story_id

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        return ["HTTP request to %s" % self._full_endpoint()]

    def _fetch_stories(self, columns, story_ids):
        def _fetch_story(story_id):
            response = requests.get(self._story_endpoint(story_id))
            response.raise_for_status()
            row = response.json()

            # Arrange the values that we were given into a dictionary of fields that
            # we need to return (the columns list).
            returned_row = {}
            for column in columns:
                returned_row[column] = row.get(column)
            return returned_row

        # Fetch the details of all stories in the story_id list. Firebase API doesn't let
        # us make batch requests, so we run this in multiple threads to speed querying up
        # (sorry, Firebase).
        with ThreadPoolExecutor(max_workers=8) as executor:
            yield from executor.map(_fetch_story, story_ids)

    def execute(self, quals, columns, sortkeys=None):
        """Main FDW entry point. Since the Firebase API doesn't support filtering, we're going
        to ignore all qualifiers here and return everything we got from it (200-500 rows,
        depending on the endpoint)."""

        response = requests.get(self._full_endpoint())
        response.raise_for_status()

        # Get all story IDs
        story_ids = response.json()

        # Actually call the get story API and return story details. We use a generator here
        # so that PostgreSQL can stop calling us if it got enough tuples, giving us free
        # support for LIMIT clauses.
        yield from self._fetch_stories(columns, story_ids)

    def __init__(self, fdw_options, fdw_columns):

        logging.basicConfig(
            format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
            level=logging.DEBUG,
        )

        # Dictionary of FDW parameters
        self.fdw_options = fdw_options

        # FDW columns columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self.endpoint = self.fdw_options["table"]

        # Which endpoint we're fetching. See https://github.com/HackerNews/API for reference.
        if self.endpoint not in [
            "topstories",
            "newstories",
            "beststories",
            "askstories",
            "showstories",
            "jobstories",
        ]:
            raise ValueError("Unsupported endpoint %s!" % self.endpoint)
