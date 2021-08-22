#!/usr/bin/env python
"""A showcase/example runner for Splitgraph"""
import codecs
import errno
import fcntl
import json
import os
import pty
import re
import struct
import subprocess
import sys
import termios
import time
import shlex

import click
import yaml

from splitgraph.core.output import Color

_ANSI_CONTROL = re.compile(r"(\x1B[@-_][0-?]*[ -/]*[@-~])")
_SPLIT = re.compile(r"(\x1B[@-_][0-?]*[ -/]*[@-~]|\s+|.)")


class RecorderOutput:
    """An stdout wrapper that emits "recordings" of the terminal session
    in Asciinema format."""

    def __init__(
        self,
        input_rate=0.03,
        delay=0.5,
        min_delay=5.0,
        max_gap=1.0,
        title=None,
        width=None,
        height=None,
        dump_key_timestamps=True,
    ):
        self.input_rate = input_rate

        # Delay as a function of #lines that were emitted; default
        # is 0.5s per line (some log output can be skimmed + we have 5s min)
        self.delay = delay
        self.max_gap = max_gap

        self.events = []
        self.current_time = 0.0

        self.dump_key_timestamps = dump_key_timestamps
        self._extra_metadata = {}

        self.header = {
            "version": 2,
            "width": 100,
            "height": None,
            "env": {"TERM": "xterm-256color", "SHELL": "/bin/zsh"},
        }
        if width:
            self.header["width"] = width
        if height:
            self.header["height"] = height

        self.header["timestamp"] = int(time.time())
        if title:
            self.header["title"] = title

        self.record = True

        self._chars_since_cls = 0
        self._cls_start_time = 0

        self.min_delay = min_delay

        # Approximate (since we don't look at ANSI control characters apart from
        # clear-screen that we produce ourselves) the maximum height of the terminal.
        # If height isn't passed, we determine it automatically.
        self._curr_height = 0
        self._max_height = 0

        self.decoder = codecs.getincrementaldecoder("UTF-8")("replace")

        # Record the final state of every screen separately as well
        self.screens = [{}]

    def _add_to_current_screen(self, field, text):
        if not self.record:
            return

        if isinstance(text, bytes):
            text = self.decoder.decode(text)

        current_screen = self.screens[-1]

        current_command = current_screen.get("command", [])
        current_output = current_screen.get("output", [])
        current_comment = current_screen.get("comment", [])

        if field == "command":

            current_command.append(text)
            current_screen[field] = current_command

            # Open new output too
            current_output.append("")
            current_screen["output"] = current_output
        elif field == "comment":
            current_comment.append(text)
            current_screen[field] = current_comment
        elif field == "output":
            current_output[-1] += text
            current_screen[field] = current_output

    def add_event(self, text, time_since_last):
        if isinstance(text, bytes):
            text = self.decoder.decode(text)

        self._curr_height += text.count("\n")
        self._max_height = max(self._max_height, self._curr_height)

        time_since_last = min(time_since_last, self.max_gap)
        self.current_time += time_since_last
        self.events.append((self.current_time, "o", text))

    def to_asciinema(self):
        header = self.header.copy()
        header["height"] = header["height"] or self._max_height + 1
        if self._extra_metadata:
            header["metadata"] = self._extra_metadata

        return json.dumps(header) + "\n" + "\n".join(json.dumps(event) for event in self.events)

    def print(self, text="", end="\n"):
        text = text + end
        print(text, end="")
        text = text.replace("\n", "\r\n")

        # Strip comment symbols + ANSI control chars
        stripped_text = text.replace("$ # ", "")
        stripped_text = _ANSI_CONTROL.sub("", stripped_text)
        stripped_text = stripped_text.rstrip()

        if self.record:
            if "$ #" in text:
                if self.dump_key_timestamps and self._curr_height == 0:
                    # Dump the first comment emitted after a clearscreen as a keypoint.
                    timestamps = self._extra_metadata.get("tss", [])

                    timestamps.append({"h": stripped_text, "ts": self.current_time})
                    self._extra_metadata["tss"] = timestamps
                self.add_event(text, time_since_last=self.input_rate)

                self._add_to_current_screen("comment", stripped_text + "\n")
            else:
                # Make sure the ANSI control sequences + whitespace are coalesced
                # (nobody wants to watch someone pressing space 50 times)
                for char in _SPLIT.findall(text):
                    self.add_event(char, time_since_last=self.input_rate)
                    self._chars_since_cls += 1

    def print_from_pipe(self, proc, fd):
        """Records output from a subprocess, including the delays and printing the output."""
        now = time.time()
        full_data = b""
        while True:
            try:
                data = os.read(fd, 512)
            except OSError as e:
                if e.errno != errno.EIO:
                    raise
                else:
                    data = b""
                # EIO means EOF on some systems
            if data:
                full_data += data
                sys.stdout.buffer.write(data)
                sys.stdout.flush()
                if self.record:
                    self.add_event(data, time_since_last=time.time() - now)
                    self._chars_since_cls += len(data)
                now = time.time()
            else:
                result = proc.poll()
                if result is not None:
                    if full_data:
                        self._add_to_current_screen("output", full_data + b"\r\n")

                    return result

    def cls(self):
        if self.record:
            if self.current_time > 0:
                # Add a delay assuming that the person has spent the whole time
                # since the text started showing up reading it.
                self.current_time += max(
                    self.delay * self._curr_height - (self.current_time - self._cls_start_time),
                    self.min_delay,
                )
            self._chars_since_cls = 0
        if self.screens[-1]:
            self.screens.append({})
        self._cls_start_time = self.current_time
        self._curr_height = 0
        self.print("\033[H\033[J", end="")


@click.command(name="example")
@click.option("--skip", type=int, help="Skip this many 'command' blocks in the file", default=0)
@click.option(
    "--no-pause", is_flag=True, default=False, help="Don't wait for user input after every block"
)
@click.option("--dump-asciinema", type=click.File("w"), default=None)
@click.option("--dump-screens", type=click.File("w"), default=None)
@click.option("--asciinema-width", type=int, default=None)
@click.option("--asciinema-height", type=int, default=None)
@click.argument("file")
def example(skip, no_pause, dump_asciinema, dump_screens, asciinema_width, asciinema_height, file):
    """Run commands in an example YAML file."""
    with open(file, "r") as f:
        commands = yaml.load(f)
    current_prompt = ""

    output = RecorderOutput(
        width=asciinema_width, height=asciinema_height, dump_key_timestamps=True
    )

    for i, block in enumerate(commands):
        if i < skip:
            continue

        if "prompt" in block:
            current_prompt = block["prompt"]
        prompt = current_prompt + (" " if current_prompt else "")

        stderr = bool(block.get("stderr", "True") == "True")
        echo = bool(block.get("echo", "True") == "True")
        wait = bool(block.get("wait", "True") == "True")
        workdir_rel = block.get("workdir", ".")
        workdir = os.path.join(os.path.dirname(os.path.realpath(file)), workdir_rel)
        output.record = bool(block.get("record", "True") == "True")

        if workdir_rel != ".":
            prompt = workdir_rel + " " + prompt

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

        env = os.environ.copy()
        for l in block["commands"]:
            output._add_to_current_screen("command", "$ " + l.rstrip("\n").replace("\n", "\r\n"))
            mo, so = pty.openpty()  # provide tty to enable line-buffering
            mi, si = pty.openpty()

            # Set terminal window size -- pretend we're 100 cols, 120 rows, 600x800 pixels.
            fcntl.ioctl(so, termios.TIOCSWINSZ, struct.pack("HHHH", 100, 120, 600, 800))

            proc = subprocess.Popen(
                l,
                shell=True,
                stdout=so,
                stderr=so if stderr else None,
                stdin=si,
                bufsize=1,
                close_fds=True,
                env=env,
                cwd=workdir,
            )
            for fd in [so, si]:
                os.close(fd)

            result = output.print_from_pipe(proc, mo)

            if result != 0:
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

    if dump_screens:
        dump_screens.write(json.dumps(output.screens))


if __name__ == "__main__":
    example()
