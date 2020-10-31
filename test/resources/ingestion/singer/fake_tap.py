#!/usr/bin/env python
import json
import os

import click


@click.command(name="tap")
@click.option("-c", "--config", type=click.File("r"))
@click.option("-s", "--state", type=click.File("r"))
@click.option("--catalog", type=click.File("r"))
@click.option("-p", "--properties", type=click.File("r"))
@click.option("-d", "--discover", is_flag=True)
def tap(state, config, catalog, properties, discover):
    basepath = os.path.dirname(__file__)
    json.load(config)

    if discover:
        with open(os.path.join(basepath, "discover.json")) as f:
            click.echo(f.read(), nl=False)
        return

    assert catalog or properties
    catalog_j = json.load(catalog or properties)
    assert len(catalog_j["streams"]) == 2

    if state:
        json.load(state)
        with open(os.path.join(basepath, "update.json")) as f:
            click.echo(f.read(), nl=False)
        return

    with open(os.path.join(basepath, "initial.json")) as f:
        click.echo(f.read(), nl=False)


if __name__ == "__main__":
    tap()
