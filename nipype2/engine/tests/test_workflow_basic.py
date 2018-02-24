import pytest, pdb
import numpy as np
import time, os

from ..node import Node
from ..workflow import Workflow
from ..auxiliary import Function_Interface

from ... import config, logging
config.enable_debug_mode()
logging.update_logging(config)

def funA(a):
    print("AAA BEFORE WAITITIN")
    time.sleep(7)
    print("AAA AFTER WAITITIN")
    return a**2

def funB(b):
    return b+2

def funC(c):
    return 10 * c

def funD(d1, d2):
    return d1 + d2

def funE(e1, e2):
    return e1 * e2

def funF():
    return 0


def test_workflow_basic_1():
    """graph: A, B"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")

    wf = Workflow(nodes=[nA, nB], name="workflow_1", workingdir="test1")
    wf.run()
    pdb.set_trace()
    assert nA.result["out"][0][0] == {"a":5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"b": 15}
    assert nB.result["out"][0][1] == 17


def test_workflow_basic_2():
    """graph: A -> C, B"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")

    wf = Workflow(nodes=[nA, nB, nC], name="workflow_2", workingdir="test2")
    wf.connect(nA, "out", nC, "c")
    wf.run()

    assert nA.result["out"][0][0] == {"a":5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"b": 15}
    assert nB.result["out"][0][1] == 17

    assert nC.result["out"][0][0] == {"a":5}
    assert nC.result["out"][0][1] == 250


def test_workflow_basic_3():
    """graph: A -> C, A -> D,  B -> D, C -> E, D -> E, F"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp")
    nE = Node(interface=Function_Interface(funE, ["out"]),
              name="nE", plugin="mp")
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF", plugin="mp")



    wf = Workflow(nodes=[nA, nB, nC, nD, nE, nF], name="workflow_3", workingdir="test3")
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "d1")
    wf.connect(nB, "out", nD, "d2")
    wf.connect(nC, "out", nE, "e1")
    wf.connect(nD, "out", nE, "e2")
    wf.run()

    assert nA.result["out"][0][0] == {"a": 5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"b": 15}
    assert nB.result["out"][0][1] == 17

    assert nC.result["out"][0][0] == {"a": 5}
    assert nC.result["out"][0][1] == 250

    assert nD.result["out"][0][0] == {"a": 5, "b": 15}
    assert nD.result["out"][0][1] == 42

    assert nE.result["out"][0][0] == {"a": 5, "b": 15}
    assert nE.result["out"][0][1] == 10500

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0
