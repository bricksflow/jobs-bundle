import sys
from argparse import ArgumentParser, Namespace


def add_kwargs(argument_parser: ArgumentParser):
    for argv in sys.argv[3:]:
        argv_parsed = argv.split("=")

        argument_parser.add_argument(argv_parsed[0])


def extract_kwargs(input_args: Namespace):
    values = input_args.__dict__
    del values["env"]

    if "help_selected" in values:
        del values["help_selected"]

    del values["command_name"]

    return values
