#!/usr/bin/env python
"""A showcase/example runner for Splitgraph"""

import subprocess
import sys

import click
import yaml

from splitgraph.commandline._common import Color


@click.command(name="example")
@click.option("--skip", type=int, help="Skip this many 'command' blocks in the file", default=0)
@click.option(
    "--no-pause", is_flag=True, default=False, help="Don't wait for user input after every block"
)
@click.argument("file")
def example(skip, no_pause, file):
    """Run commands in an example YAML file."""
    with open(file, "r") as f:
        commands = yaml.load(f)
    current_prompt = ""

    for i, block in enumerate(commands):
        if i < skip:
            continue

        print("\033[H\033[J")
        if "prompt" in block:
            current_prompt = block["prompt"]
        prompt = current_prompt + (" " if current_prompt else "")

        stderr = bool(block.get("stderr", "True") == "True")
        echo = bool(block.get("echo", "True") == "True")
        wait = bool(block.get("wait", "True") == "True")

        if echo:
            print(
                Color.BOLD + Color.DARKCYAN + prompt + Color.RED + "$ " + block["commands"][0],
                end="",
            )
            if len(block["commands"]) > 1:
                print()
            print(
                "\n".join(
                    (" " * len(prompt)) + "$ " + t.rstrip("\n") for t in block["commands"][1:]
                )
                + Color.END,
                end="",
            )
            if not no_pause:
                input()

        for l in block["commands"]:
            result = subprocess.run(
                l, shell=True, stderr=(subprocess.STDOUT if stderr else subprocess.DEVNULL)
            )
            if result.returncode != 0:
                exit(1)

        if wait and not no_pause:
            input(
                Color.BOLD
                + Color.DARKCYAN
                + current_prompt
                + (" " if current_prompt else "")
                + Color.RED
                + "$ "
                + Color.END
            )


if __name__ == "__main__":
    example()
