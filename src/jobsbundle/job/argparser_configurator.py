import sys
from itertools import chain
from argparse import ArgumentParser, Namespace

from typing import Dict, Set


def add_kwargs(argument_parser: ArgumentParser):
    default_actions_set = __get_default_actions(argument_parser)
    split_kwargs = (arg.split("=")[0] for arg in sys.argv[3:])

    kwargs = filter(lambda kwarg: kwarg not in default_actions_set, split_kwargs)

    for kwarg in kwargs:
        argument_parser.add_argument(kwarg)


def extract_kwargs(input_args: Namespace) -> Dict[str, str]:
    values = input_args.__dict__

    values.pop("help_selected", None)
    values.pop("command_name", None)

    return values


def __get_default_actions(argument_parser: ArgumentParser) -> Set[str]:
    option_strings_lists = (action.option_strings for action in argument_parser._action_groups[1]._group_actions)

    return set(chain.from_iterable(option_strings_lists))
