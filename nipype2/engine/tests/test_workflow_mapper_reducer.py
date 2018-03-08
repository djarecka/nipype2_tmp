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
    print("A Before Waiting")
    time.sleep(5)
    print("A After Waiting")
    return a**2

def funB(b):
    return b+2

def funC(c):
    return 10 * c

def funD(d1, d2):
    print("IM IN D", d1, d2)
    return d1 + d2

def funE(e1, e2):
    return e1 * e2

def funF():
    return 0


def test_workflow_reducer_1():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", reducer="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1", workingdir="test_reducer_1")
    wf.run()

    expected = [({"a":3}, [({"a":3}, 9)]), ({"a":4}, [({"a":4}, 16)]), ({"a":5}, [({"a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]


def test_workflow_reducer_1a():
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), reducer="d1",
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_1a", workingdir="test_reducer_1a")
    wf.run()

    expected = [({"d1":3}, [({"d1":3, "d2": 10}, 13), ({"d1":3, "d2": 40}, 43)]),
                ({"d1": 4}, [({"d1":4, "d2":20}, 24)]),
                ({"d1": 5}, [({"d1":5, "d2":30}, 35)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

