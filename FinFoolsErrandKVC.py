#!/usr/bin/env python3
# Get and work with data about MFs, indexes, stocks, or any other event/entity, etc,
# in _BLIND_ AND _STUPID_ ways.
# HanishKVC, 2021
# GPL
#

import sys
import calendar
import os
import numpy
import matplotlib.pyplot as plt
import time
import traceback
import readline
import warnings
import tabcomplete as tc
import hlpr
import datasrc
import loadfilters
import procedb
import plot
import edb
import stocks


"""
Usage scenario
    edb.fetch(2010, 2021)
    edb.load(2013, 20190105)
    # explore option 1
    procedb.infoset1_prep()
    procedb.infoset1_result(['open elss'], ['direct'])
    procedb.infoset1_result(['open equity large', 'open equity large mid', 'open equity flexi', 'open equity multi', 'open equity elss'], ['direct'])
    # explore option 2
    procedb.ops(['srel=srel(data)', 'roll3Y=roll1095(data)'])
    edb.search(['match name tokens1', 'match name tokens2'])
    procedb.anal_simple('srel', 'srel_retpa', 'top')
    procedb.anal_simple('roll3Y', 'roll_avg', 'top')
    plot.data('srel', [ entCode1, entCode2 ])
    plot.show()
    quit()
"""

gbDEBUG = False
FINFOOLSERRAND_BASE = None



def setup_paths():
    """
    Account for FINFOOLSERRAND_BASE env variable if set
    """
    global FINFOOLSERRAND_BASE
    FINFOOLSERRAND_BASE = os.environ.get('FINFOOLSERRAND_BASE',"~/")
    print("INFO:Main:setup_paths:", FINFOOLSERRAND_BASE)


def setup_modules():
    edb.setup(FINFOOLSERRAND_BASE)


def setup():
    setup_paths()
    setup_modules()


def session_save(sessionName):
    """
    Save current gEntDB.data-gEntDB.meta into a pickle, so that it can be restored fast later.
    """
    edb.session_save(sessionName)


def session_restore(sessionName):
    """
    Restore a previously saved gEntDB.data-gEntDB.meta fast from a pickle.
    Also setup the modules used by the main logic.
    """
    edb.session_restore(sessionName)


def input_multi(prompt="OO>", altPrompt="...", theFile=None):
    """
    Allow reading a single line or multiline of python block
    either from console or from the file specified.

    If user is entering a multiline python block, the program
    will show a different prompt to make it easy for the user
    to identify the same.

    Entering a empty line or a line with a smaller indentation
    than that used when multiline block entry started, will
    lead to the logic getting out of the multiline input mode.

    It allows user to split lists, sets, dictionary etc to be
    set across multiple lines, provided there is ',' as the
    last char in the inbetween lines.

    It allows if-else or if-elif-else multiline blocks.
    """
    lines = ""
    bMulti = False
    bIf = False
    lineCnt = 0
    refStartWS = 0
    while True:
        if theFile == None:
            line = input(prompt)
        else:
            line = theFile.readline()
            if line == '':
                theFile=None
                continue
        if prompt != altPrompt:
            prompt = altPrompt
        lineCnt += 1
        lineStripped = line.strip()
        if (lineCnt == 1):
            lines = line
            if (lineStripped != "") and (lineStripped[-1] in ':,\\'):
                if lineStripped.split()[0] == 'if':
                    bIf = True
                bMulti = True
                continue
            else:
                break
        else:
            if lineStripped == "":
                break
            curStartWS = len(line) - len(line.lstrip())
            #print(curStartWS)
            if (lineCnt == 2):
                refStartWS = curStartWS
            lines = "{}\n{}".format(lines,line)
            if (refStartWS > curStartWS):
                if bIf and (lineStripped.split()[0] in [ 'else:', 'elif']):
                    continue
                break
    return lines


gbREPLPrint = True
def do_run(theFile=None):
    """
    Run the REPL logic of this program.
    Read-Eval-Print Loop

    NOTE: If a script file is passed to the logic, it will fall back to
    interactive mode, once there are no more commands in the script file.
        Script file can use quit() to exit the program automatically
        if required.

    NOTE: One can control printing of REPL, by controlling gbREPLPrint.
    NOTE: User can suppress auto printing of individual python statements
            entered into interactive mode by suffixing them with ';'
    """
    bQuit = False
    while not bQuit:
        bPrint = False
        try:
            #breakpoint()
            cmd = input_multi(theFile=theFile)
            if gbREPLPrint:
                cmdStripped = cmd.strip()
                if (cmdStripped != "") and ('\n' not in cmd) and (cmdStripped[-1] != ';'):
                    cmd = "_THE_RESULT = {}".format(cmd)
                    bPrint=True
            exec(cmd,globals())
            if bPrint and (type(_THE_RESULT) != type(None)):
                print(_THE_RESULT)
        except:
            excInfo = sys.exc_info()
            if excInfo[0] == SystemExit:
                break
            traceback.print_exc()


def handle_args():
    """
    Logic to handle the commandline arguments
    """
    if sys.argv[1].endswith(".ffe"):
        print("INFO:Running ", sys.argv[1])
        f = open(sys.argv[1])
        do_run(f)
    else:
        edb.fetch_data(sys.argv[1], sys.argv[2])
        edb.load_data(sys.argv[1], sys.argv[2])
        procedb.ops('roll1095=roll1095(data)')
        procedb.anal_simple('roll1095', 'roll_avg')


#
# The main flow starts here
#
print("FinFoolsErrandKVC: A stupid exploration of multiple sets of numbers (MFs/Indexes/...) data")
print("License: GPL")
print("This is ONLY for EXPERIMENTING and WASTING some FREE TIME and NOTHING MORE...")
input("PLEASE DO NOT USE THIS PROGRAM TO MAKE ANY DECISIONS OR INFERENCES OR ...")

setup()
if len(sys.argv) > 1:
    handle_args()
else:
    do_run()

