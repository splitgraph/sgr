#!/usr/bin/env python
"""A showcase/example runner for Splitgraph"""
import json
import re
import subprocess
import sys
import time

import click
import yaml

from splitgraph.commandline.common import Color


_ASCIINEMA_PRELUDE = {
    "version": 2,
    "width": 100,
    "height": 40,
    "env": {"TERM": "xterm-256color", "SHELL": "/bin/zsh"},
}
_ANSI_ESCAPE = re.compile(r"(\x1B[@-_][0-?]*[ -/]*[@-~]|\s+|.)")


class RecorderOutput:
    """An stdout wrapper that emits "recordings" of the terminal session
    in Asciinema format."""

    def __init__(self, input_rate=0.03, delay=1.0 / 70, min_delay=5.0, max_gap=1.0, title=None):
        self.input_rate = input_rate

        # Delay as a function of #characters that were emitted;
        # average human reading speed is 25 and there's
        # stuff like image hashes they will glance over).
        self.delay = delay
        self.max_gap = max_gap

        self.events = []
        self.current_time = 0.0

        self.header = _ASCIINEMA_PRELUDE.copy()
        self.header["timestamp"] = int(time.time())
        if title:
            self.header["title"] = title

        self.record = True

        self._chars_since_cls = 0
        self._cls_start_time = 0
        self.min_delay = min_delay

    def add_event(self, text, time_since_last):
        if isinstance(text, bytes):
            text = text.decode("unicode_escape")
        time_since_last = min(time_since_last, self.max_gap)
        self.current_time += time_since_last
        self.events.append((self.current_time, "o", text))

    def to_asciinema(self):
        return (
            json.dumps(self.header) + "\n" + "\n".join(json.dumps(event) for event in self.events)
        )

    def print(self, text="", end="\n"):
        text = text + end
        print(text, end="")
        text = text.replace("\n", "\r\n")
        if self.record:
            if "$ # " in text:
                self.add_event(text, time_since_last=self.input_rate)
            else:
                # Make sure the ANSI control sequences + whitespace are coalesced
                # (nobody wants to watch someone pressing space 50 times)
                for char in _ANSI_ESCAPE.findall(text):
                    self.add_event(char, time_since_last=self.input_rate)
                    self._chars_since_cls += 1

    def print_from_pipe(self, proc):
        """Records output from a subprocess, including the delays and printing the output."""
        now = time.time()
        while True:
            line = proc.stdout.readline()
            if line:
                sys.stdout.buffer.write(line)
                sys.stdout.flush()
                line = line.replace(b"\n", b"\r\n")
                if self.record:
                    self.add_event(line, time_since_last=time.time() - now)
                    self._chars_since_cls += len(line)
                now = time.time()
            else:
                result = proc.poll()
                if result is not None:
                    return result

    def cls(self):
        if self.record:
            if self.current_time > 0:
                # Add a delay assuming that the person has spent the whole time
                # since the text started showing up reading it.
                self.current_time += max(
                    self.delay * self._chars_since_cls - (self.current_time - self._cls_start_time),
                    self.min_delay,
                )
            self._chars_since_cls = 0
        self._cls_start_time = self.current_time
        self.print("\033[H\033[J", end="")


@click.command(name="example")
@click.option("--skip", type=int, help="Skip this many 'command' blocks in the file", default=0)
@click.option(
    "--no-pause", is_flag=True, default=False, help="Don't wait for user input after every block"
)
@click.option("--dump-asciinema", type=click.File("w"), default=None)
@click.argument("file")
def example(skip, no_pause, dump_asciinema, file):
    """Run commands in an example YAML file."""
    with open(file, "r") as f:
        commands = yaml.load(f)
    current_prompt = ""

    output = RecorderOutput()

    for i, block in enumerate(commands):
        if i < skip:
            continue

        if "prompt" in block:
            current_prompt = block["prompt"]
        prompt = current_prompt + (" " if current_prompt else "")

        stderr = bool(block.get("stderr", "True") == "True")
        echo = bool(block.get("echo", "True") == "True")
        wait = bool(block.get("wait", "True") == "True")
        output.record = bool(block.get("record", "True") == "True")

        output.cls()
        if echo:
            output.print(
                Color.BOLD + Color.DARKCYAN + prompt + Color.RED + "$ " + block["commands"][0],
                end="",
            )
            if len(block["commands"]) > 1:
                output.print()
            for t in block["commands"][1:]:
                output.print((" " * len(prompt)) + "$ " + t.rstrip("\n"))
            output.print(Color.END, end="")
            if not no_pause:
                input()
            else:
                output.print()

        for l in block["commands"]:
            proc = subprocess.Popen(
                l, shell=True, stdout=subprocess.PIPE, stderr=(subprocess.PIPE if stderr else None)
            )
            if output.print_from_pipe(proc) != 0:
                exit(1)

        output.print(
            Color.BOLD
            + Color.DARKCYAN
            + current_prompt
            + (" " if current_prompt else "")
            + Color.RED
            + "$ "
            + Color.END,
            end="",
        )
        if wait and not no_pause:
            input()

    if dump_asciinema:
        recording = output.to_asciinema()
        dump_asciinema.write(recording)


if __name__ == "__main__":
    example()
