#!/usr/bin/env python3
# A simple TabCompletion logic, married to readline
# HanishKVC, 2021

import readline


gMeta = None
L1 = [ "load_data", "fetch_data", "lookat_data", "search_data",
        "show_plot",
        "enttypes.list", "enttypes.members",
        "procdata_ex", "plot_data", "procdata_mabeta",
        "loadfilters_setup", "loadfilters_list",
        "analdata_simple",
        "session_save", "session_restore",
        "infoset1_prep", "infoset1_result",
        "quit"
        ]


def matching_list(curPart, text):
    match = []
    if gMeta == None:
        gMeta = {'entTypes':[]}
    for x in gMeta['entTypes']:
        if x.startswith(curPart):
            match.append(text+x[len(curPart):])
    return match


def complete(text, state):
    """
    Handle the tab completion
    """
    curLine = readline.get_line_buffer()
    curTokens = curLine.split()
    numTokens = len(curTokens)
    if (numTokens < 1) or ((numTokens == 1) and (text != "")):
        match = [x for x in L1 if x.startswith(text)]
        return match[state]
    if curLine.startswith("enttypes_members("):
        data = curLine[16:].lstrip()
        if data[0] == '"':
            data = data[1:]
        match = matching_list(data, text)
        return match[state]
    return None


def rlcompleter():
    import rlcompleter



readline.parse_and_bind("tab: complete")
readline.set_completer(complete)

