"""
Generates a skeleton with a series of Click commands arranged in a directory.
"""

import os
import shutil

import click

# Map category to Click commands -- maybe eventually we'll read this dynamically...
STRUCTURE = {
    "Image management/creation": ["checkout", "commit", "tag", "import"],
    "Image information": ["log", "diff", "show", "sql", "status"],
    "Miscellaneous": ["mount", "rm", "init", "cleanup", "prune", "config"],
    "Sharing images": ["clone", "push", "pull", "publish", "upstream"],
    "Splitfile execution": ["build", "rebuild", "provenance"],
}


def _make_toctree(items, glob=False):
    # yeah, we could use sphinx's builder here
    return ".. toctree::\n" + ("    :glob:" if glob else "") + "\n\n    " + "\n    ".join(items)


def _make_header(header):
    return header + "\n" + "=" * len(header) + "\n"


def _make_click(command_fn):
    return ".. click:: splitgraph.commandline:" + command_fn + "_c\n   :prog: " + command_fn


def _slug_section(section):
    return section.lower().replace(" ", "_").replace("/", "_")


@click.command(name="main")
@click.argument("output", default="commands", required=False)
@click.option("-f", "--force", default=False, is_flag=True)
def main(output, force):
    if os.path.exists(output):
        if not force:
            raise click.ClickException("%s already exists, pass -f" % output)
        else:
            print("Removing %s" % output)
            shutil.rmtree(output)

    os.mkdir(output)
    # Make the introductory page
    print("Making %s/%s.rst..." % (output, output))
    with open(os.path.join(output, output + ".rst"), "w") as f:
        f.write(_make_header("sgr command line client"))
        f.write(_make_toctree([_slug_section(s) for s in STRUCTURE.keys()]))

    for section, commands in STRUCTURE.items():
        section_slug = _slug_section(section)
        os.mkdir(os.path.join(output, section_slug))
        section_path = os.path.join(output, section_slug + ".rst")
        # Make the section toctree
        print("Making %s..." % section_path)
        with open(section_path, "w") as f:
            f.write(_make_header(section))
            f.write(_make_toctree([section_slug + "/*"], glob=True))

        # Make the per-command page
        for c in commands:
            command_path = os.path.join(output, section_slug, c + ".rst")
            print("Making %s..." % command_path)
            with open(command_path, "w") as f:
                f.write(_make_click(c))

    print("Done, add %s/%s to your toctree" % (output, output))


if __name__ == "__main__":
    main()
