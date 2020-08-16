#!/usr/bin/env python
import errno
import os
import shlex
import subprocess

import click


@click.command(name="rebuild")
@click.option("--skip-gif", type=bool, is_flag=True, default=False)
@click.option("--skip-record", type=bool, is_flag=True, default=False)
@click.option(
    "--extra-recorder-args",
    type=str,
    default="",
    help="Extra arguments to pass to run_example.py, e.g. --height 60 --width 100",
)
@click.option(
    "--extra-gif-args",
    type=str,
    default="-s1 -S1 -t solarized-dark",
    help="Extra arguments to pass to asciicast2gif, -S1 is recommended (doesn't double the GIF's resolution),"
    "otherwise Imagemagick takes up ~6GB of RAM merging the frames",
)
@click.option("--gif-stylesheet", type=str, default=None)
@click.option("--output-path", type=str, default="asciinema")
@click.option("--config", type=str, default=None, help=".sgconfig file to be used by the recorder")
@click.argument("directory")
def rebuild(
    skip_gif,
    skip_record,
    extra_recorder_args,
    extra_gif_args,
    gif_stylesheet,
    output_path,
    config,
    directory,
):
    workdir = os.path.join(os.path.dirname(__file__), directory)

    asciinema_dir = os.path.abspath(output_path)
    try:
        os.mkdir(asciinema_dir)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        pass

    asciinema_basename = os.path.basename(directory)
    asciinema_cast = asciinema_basename + ".cast"
    asciinema_path = os.path.join(asciinema_dir, asciinema_cast)

    if not skip_record:
        # Make sure the output of whatever run_example is running doesn't get buffered
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        if config:
            # Use absolute path for SG_CONFIG_FILE since we're
            # running run_example from a different directory.
            env["SG_CONFIG_FILE"] = os.path.abspath(config)
        # Disable the update check
        env["SG_UPDATE_FREQUENCY"] = "0"
        args = [
            "../run_example.py",
            "example.yaml",
            "--no-pause",
            "--dump-asciinema",
            asciinema_path,
        ] + shlex.split(extra_recorder_args)
        click.echo("Creating Asciinema recording...")
        click.echo(" ".join(args))
        subprocess.check_call(args, env=env, cwd=workdir)

    if skip_gif:
        return

    # Run asciicast2gif in Docker: entrypoint args are
    # [EXTRA_ARGS] CAST_FILE OUTPUT_FILE
    # where EXTRA_ARGS can be e.g. -s1 -S1 -t solarized-dark etc.
    args = ["docker", "run", "--rm", "-v", asciinema_dir + ":" + "/data", "-v"]

    if gif_stylesheet:
        args.append(os.path.abspath(gif_stylesheet) + ":" + "/app/page/asciinema-player.css")

    args += (
        ["asciinema/asciicast2gif"]
        + shlex.split(extra_gif_args)
        + [asciinema_cast, asciinema_basename + ".gif"]
    )
    click.echo("Creating GIF file...")
    click.echo(" ".join(args))
    subprocess.check_call(args, cwd=workdir)


if __name__ == "__main__":
    rebuild()
