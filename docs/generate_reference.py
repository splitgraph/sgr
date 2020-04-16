"""
Generates the sgr documentation in Markdown format
"""
import json
import os
import shutil

import click

import splitgraph.commandline as cmd
from splitgraph.commandline.cloud import register_c, login_c, curl_c, login_api_c
from splitgraph.commandline.engine import (
    add_engine_c,
    delete_engine_c,
    list_engines_c,
    start_engine_c,
    stop_engine_c,
    log_engine_c,
    configure_engine_c,
    upgrade_engine_c,
    version_engine_c,
)
from splitgraph.commandline.ingestion import csv_export, csv_import

# Map category to Click commands -- maybe eventually we'll read this dynamically...
from splitgraph.config.keys import KEYS, KEY_DOCS, DEFAULTS

STRUCTURE = [
    ("Image management/creation", ["checkout", "commit", "tag", "import", "reindex"]),
    ("Image information", ["log", "diff", "object", "objects", "show", "table", "sql", "status"]),
    (
        "Engine management",
        [
            "engine add",
            "engine delete",
            "engine list",
            "engine start",
            "engine stop",
            "engine upgrade",
            "engine log",
            "engine configure",
            "engine version",
        ],
    ),
    ("Data import/export", ["csv export", "csv import", "mount"]),
    ("Miscellaneous", ["rm", "init", "cleanup", "prune", "config", "dump", "eval", "upgrade"]),
    ("Sharing images", ["clone", "push", "pull", "upstream"]),
    ("Splitfile execution", ["build", "rebuild", "provenance", "dependents"]),
    ("Splitgraph Cloud", ["cloud register", "cloud login", "cloud login-api", "cloud curl"]),
]

# Map command names to Click command instances where they don't have the expected (cmd_name + '_c') format
STRUCTURE_CMD_OVERRIDE = {
    "csv export": csv_export,
    "csv import": csv_import,
    "engine add": add_engine_c,
    "engine delete": delete_engine_c,
    "engine list": list_engines_c,
    "engine start": start_engine_c,
    "engine upgrade": upgrade_engine_c,
    "engine stop": stop_engine_c,
    "engine log": log_engine_c,
    "engine configure": configure_engine_c,
    "engine version": version_engine_c,
    "cloud register": register_c,
    "cloud login": login_c,
    "cloud login-api": login_api_c,
    "cloud curl": curl_c,
}


def _emit_document_header(doc_id, title):
    return "---\nid: %s\ntitle: %s\n---\n\n" % (doc_id, title)


def _emit_header(header, level=1):
    return "#" * level + " " + header


def _emit_argument(argument):
    return argument.make_metavar()


def _emit_command_invocation(command, name, prefix="sgr "):
    # e.g. sgr import [OPTIONS] IMAGE_SPEC ...
    result = (
        "```"
        + prefix
        + name
        + " [OPTIONS] "
        + " ".join(_emit_argument(a) for a in command.params if isinstance(a, click.Argument))
        + "```\n"
    )
    return result


def _emit_command_options(command):
    help_records = [
        p.get_help_record(click.Context(command))
        for p in command.params
        if isinstance(p, click.Option)
    ]

    if help_records:
        result = "\n\n" + _emit_header("Options", level=3) + "\n\n"
        result += "\n".join(
            "  * **`%s`**: %s" % (option, option_help) for option, option_help in help_records
        )
        return result
    else:
        return ""


def _emit_mdx_metadata(doc_id, title, sidebar_title=None):
    sidebar_title = sidebar_title or title
    return (
        "export const meta = {id: %s, title: %s, sidebarTitle: %s};"
        % (json.dumps(doc_id), json.dumps(title), json.dumps(sidebar_title))
        + "\n"
    )


def _emit_dir_metadata(title):
    return json.dumps({"title": title}) + "\n"


def _emit_command(command_name):
    command = STRUCTURE_CMD_OVERRIDE.get(command_name)
    if not command:
        command = getattr(cmd, command_name + "_c")
    result = _emit_command_invocation(command, command_name)
    # Future: move examples under options?
    result += "\n" + command.help.replace("Examples:", "### Examples")
    result += _emit_command_options(command)
    return result


def _slug_section(section):
    return section.lower().replace(" ", "_").replace("/", "_")


@click.command(name="sgr")
@click.argument("output", default="../docs/sgr", required=False)
@click.option("-f", "--force", default=False, is_flag=True)
def sgr(output, force):
    if os.path.exists(output):
        if not force:
            raise click.ClickException("%s already exists, pass -f" % output)
        else:
            print("Removing %s" % output)
            shutil.rmtree(output)

    os.mkdir(output)

    for section_no, (section, commands) in enumerate(STRUCTURE):
        section_slug = "%04d_%s" % (section_no, _slug_section(section))
        section_path = os.path.join(output, section_slug)
        os.mkdir(section_path)
        with open(os.path.join(section_path, "metadata.json"), "w") as f:
            f.write(_emit_dir_metadata(title=section))

        for command_no, command in enumerate(commands):
            doc_id = _slug_section(command)
            doc_filename = "%04d_%s.mdx" % (command_no, doc_id)
            command_path = os.path.join(section_path, doc_filename)
            print("Making %s: %s..." % (command_path, command))
            with open(command_path, "w") as f:
                f.write(
                    _emit_mdx_metadata(doc_id=doc_id, title="sgr " + command, sidebar_title=command)
                )
                f.write("\n")
                f.write(_emit_command(command) + "\n")

    print("Done.")


@click.command(name="config")
@click.argument("output", default="../docs/0100_config_flag_reference.mdx", required=False)
@click.option("-f", "--force", default=False, is_flag=True)
def config(output, force):
    if os.path.exists(output):
        if not force:
            raise click.ClickException("%s already exists, pass -f" % output)

    with open(output, "w") as f:
        f.write(
            _emit_mdx_metadata(doc_id="config_flag_reference", title="Configuration flag reference")
        )
        f.write("\n\n")

        for key in KEYS:
            if key not in KEY_DOCS:
                continue

            docstring = KEY_DOCS[key]
            f.write(f"### `{key}`\n{docstring}")
            if DEFAULTS.get(key):
                f.write(f"  \n**Default**: `{DEFAULTS[key]}`")
            f.write("\n\n")


@click.group(name="main")
def main():
    """Generate dynamic Markdown documentation for Splitgraph to be embedded into the website."""


main.add_command(sgr)
main.add_command(config)


if __name__ == "__main__":
    main()
