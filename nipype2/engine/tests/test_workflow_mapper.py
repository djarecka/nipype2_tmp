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
    return d1 + d2

def funE(e1, e2):
    return e1 * e2

def funF():
    return 0


def test_workflow_mapper_1():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1", workingdir="test_mapper_1")
    wf.run()

    expected = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]

    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]


def test_workflow_mapper_2():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nB = Node(inputs={"b": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB", plugin="mp")

    wf = Workflow(nodes=[nA, nB], name="workflow_2", workingdir="test_mapper_2")
    wf.run()

    expected = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]

    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    assert nB.result["out"][0][0] == {"b": 15}
    assert nB.result["out"][0][1] == 17


def test_workflow_mapper_3():
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", plugin="mp")

    wf = Workflow(nodes=[nA, nC], name="workflow_3", workingdir="test_mapper_3")
    wf.connect(nA, "out", nC, "c")
    wf.run()

    expected = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25),
                ({"a": 6}, 36), ({"a": 7}, 49), ({"a": 8}, 64)]

    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]
