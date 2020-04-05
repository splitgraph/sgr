# Template for Splitgraph examples

In general, you want:

* A docker-compose file with the engine and any extra components required (e.g. MongoDB)
* Change the .sgconfig file to have the required configuration of the engine (e.g extra mount handlers)
* An example.yaml file that runs the example (see the file in the current directory for inspiration):
  * Clean up (`docker-compose down -v`) before and after running the example
  * Examples are supposed to be run from the same directory that the YAML is located in
  * If one command in a block fails (returns nonzero), the whole example fails
  * There's a test suite in `test/` that automatically scans through all directories with example.yaml and
    runs them without pausing (to check that the example can be run through and doesn't crash). Run `pytest`
    from the `examples/` directory to run all tests or `pytest -k folder_name` to run just one.

## Running the example

`../run_example.py example.yaml` and press ENTER when prompted to go through the steps.
