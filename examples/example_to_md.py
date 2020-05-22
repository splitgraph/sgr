#!/usr/bin/env python
import json
import re
from typing import List, Dict, Any

import click

try:
    import pyte
except ImportError:
    raise click.UsageError("pyte is required (pip install pyte)!")


def _terminal_buffer_to_str(buffer: List[str]) -> str:
    result = []
    found_data = False

    # Iterate from the bottom of the screen to drop empty lines
    for line in reversed(buffer):
        # Strip hanging spaces
        line = re.sub(r"(\S*)\s+$", r"\1", line)

        # Strip empty commands that the recorder put there
        if line == "$":
            line = ""

        # Skip empty lines until we reach some data
        if line:
            found_data = True
        if found_data or line:
            result.append(line)
    return "\n".join(reversed(result))


def emit_command_output(chars, screen_width=110, screen_height=120):
    screen = pyte.Screen(columns=screen_width, lines=screen_height)
    stream = pyte.Stream(screen)
    stream.feed(chars)
    return _terminal_buffer_to_str(screen.display)


def emit_screen(screen: Dict[str, Any], output_markdown_language="shell-session"):
    result = ""
    if not screen:
        return result

    comment = screen.get("comment")
    if comment:
        result += "\n\n" + "".join(comment) + "\n"

    code_block = ""
    for command, output in zip(screen.get("command", []), screen.get("output", [])):

        if command:
            actual_output = emit_command_output(command).strip()
            if actual_output.startswith("$ #"):
                continue
            if "\\" in actual_output:
                # Hack since prisma shell-session / bash breaks on backslashes
                # (I tried escaping them and it breaks on 2, 4 and 8 backslashes).
                output_markdown_language = "python"
            code_block += actual_output + "\n"

        if output:
            actual_output = emit_command_output(output).strip()
            if "\\" in actual_output:
                output_markdown_language = "python"
            code_block += actual_output + "\n"
        code_block += "\n\n"

    code_block = code_block.strip()
    if code_block:
        result += f"\n```{output_markdown_language}\n{code_block}\n```\n"

    return result


@click.command(name="to_markdown")
@click.argument("file", type=click.File("r"))
@click.argument("output", type=click.File("w"))
def to_markdown(file, output):
    """Convert the screen.json file produced by run_example.py --dump-screens
    into Markdown.
    """
    screens = json.load(file)

    for screen in screens:
        output.write(emit_screen(screen))


if __name__ == "__main__":
    to_markdown()
