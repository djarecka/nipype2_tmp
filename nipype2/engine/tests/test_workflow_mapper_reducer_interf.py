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

def fun_sum(mylist):
    time.sleep(2)
    return(sum(mylist))


def test_workflow_reducer_interf_1():
    """graph: A"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", reducer="a",
              interface=Function_Interface(funA, ["out"]),
              reducer_fun_inp=(fun_sum, "out"),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1", workingdir="test_reducer_interf_1")
    wf.run()

    expected = [({"a":3}, [({"a":3}, 9)]), ({"a":4}, [({"a":4}, 16)]), ({"a":5}, [({"a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_redu_interf = [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25)]
    for i, res in enumerate(expected_redu_interf):
        assert nA.result_redu_interf["red_out"][i][0] == res[0]
        assert nA.result_redu_interf["red_out"][i][1] == res[1]




def test_workflow_reducer_interf_2():
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])},
              mapper=("d1", "d2"), interface=Function_Interface(funD, ["out"]),
              reducer="d1", reducer_fun_inp=(fun_sum, "out"),
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_1a", workingdir="test_reducer_interf_2")
    wf.run()

    expected = [({"d1":3}, [({"d1":3, "d2": 10}, 13), ({"d1":3, "d2": 40}, 43)]),
                ({"d1": 4}, [({"d1":4, "d2":20}, 24)]),
                ({"d1": 5}, [({"d1":5, "d2":30}, 35)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({"d1":3}, 56), ({"d1":4}, 24), ({"d1":5}, 35)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_redu_interf["red_out"][i][0] == res[0]
        assert nD.result_redu_interf["red_out"][i][1] == res[1]



def test_workflow_reducer_interf_3():
    """graph: A -> D"""
    nA = Node(inputs={"a": np.array([3, 4, 3])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nD = Node(inputs={"d1": np.array([10, 20, 30])},
              mapper=("a", "d1"), reducer="a",
              reducer_fun_inp=(fun_sum, "out"),
              interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nA, nD], name="workflow_4", workingdir="test_reducer_interf_3")
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    expected = [({"a":3}, [({"a":3, "d1": 10}, 19), ({"a":3, "d1": 30}, 39)]),
                ({"a": 4}, [({"a":4, "d1":20}, 36)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({"a":3}, 58), ({"a":4}, 36)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_redu_interf["red_out"][i][0] == res[0]
        assert nD.result_redu_interf["red_out"][i][1] == res[1]
