#!/usr/bin/env python3
# A simple TabCompletion logic, married to readline
# HanishKVC, 2021

import readline


L1 = [ "load_data", "fetch_data", "lookat_data", "search_data",
        "show_plot", "mftypes_list",
        "quit"
        ]


def complete(text, state):
    match = [x for x in L1 if x.startswith(text)]
    return match[state]


readline.parse_and_bind("tab: complete")
readline.set_completer(complete)

