#!/usr/bin/env python3
# A simple TabCompletion logic, married to readline
# HanishKVC, 2021

import readline


L1 = [ "load_data", "fetch_data", "lookat_data", "search_data",
        "show_plot", "mftypes_list",
        "quit"
        ]


def complete(text, state):
    """
    Handle the tab completion
    """
    curTokens = readline.get_line_buffer().split()
    if (len(curTokens) < 1) or ((len(curTokens) == 1) and (text != "")):
        match = [x for x in L1 if x.startswith(text)]
        return match[state]

    return None


readline.parse_and_bind("tab: complete")
readline.set_completer(complete)

