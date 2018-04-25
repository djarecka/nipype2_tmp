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


def test_workflow_join_1():
    """graph: A"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", joinByKey=["a"],
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1", workingdir="test_join_1")
    wf.run()
    #pdb.set_trace()

    expected = [({}, [({"a": 3}, 9), ({"a": 4}, 16), ({"a": 5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]


def test_workflow_join_1a():
    """graph: A; should be the same as 1"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", join=True,
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")

    wf = Workflow(nodes=[nA], name="workflow_1a", workingdir="test_join_1a")
    wf.run()

    expected = [({}, [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]



def test_workflow_join_2():
    """graph: D; scalar mapper"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_2", workingdir="test_join_2")
    wf.run()

    expected = [
        ({"d1":3}, [({"d1":3, "d2":10}, 13),({"d1":3, "d2":40}, 43)]), # TOASK: should this be merged
        ({"d1":4}, [({"d1":4, "d2":20}, 24)]),
        ({"d1":5}, [({"d1":5, "d2":30}, 35)])
        ]
    #pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


def test_workflow_join_2a():
    """graph: D; scalar mapper; it will be exactly the same output as 2"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_2a", workingdir="test_join_2a")
    wf.run()

    expected = [
            ({"d2":10}, [({"d1":3, "d2":10}, 13)]),
            ({"d2":20}, [({"d1":4, "d2":20}, 24)]),
            ({"d2":30}, [({"d1":5, "d2":30}, 35)]),
            ({"d2":40}, [({"d1":3, "d2":40}, 43)])
        ]
    #pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


def test_workflow_join_2b():
    """graph: D; scalar mapper; it will be exactly the same output as 2"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), join=True,
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_2b", workingdir="test_join_2b")
    wf.run()

    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":40}, 43), #the order like in 2
                      ({"d1":4, "d2":20}, 24), ({"d1":5, "d2":30}, 35)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]



def test_workflow_join_3():
    """graph: A -> D, joinByKey in the second key"""
    nA = Node(inputs={"a": np.array([3, 4, 3])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA", plugin="mp")
    nD = Node(inputs={"d1": np.array([10, 20, 30])},
              mapper=("a", "d1"), joinByKey=["a"],
              interface=Function_Interface(funD, ["out"]),
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nA, nD], name="workflow_3", workingdir="test_join_3")
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    expected = [
        ({"d1":10}, [({"a":3, "d1": 10}, 19)]),
        ({"d1":20}, [({"a":4, "d1":20}, 36)]),
        ({"d1":30}, [({"a":3, "d1": 30}, 39)])
            ]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]



def test_workflow_join_4():
    """graph: D; mapper: vector """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_4", workingdir="test_join_4")
    wf.run()

    expected = [({"d1":3}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":20}, 23)]),
                ({"d1":4}, [({"d1":4, "d2":10}, 14), ({"d1":4, "d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]



def test_workflow_join_4a():
    """graph: D; mapper: vector; this is 'transpose' to 4 """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_4a", workingdir="test_join_4a")
    wf.run()

    expected = [({"d2":10}, [({"d1":3, "d2":10}, 13), ({"d1":4, "d2":10}, 14)]),
                ({"d2":20}, [({"d1":3, "d2":20}, 23), ({"d1":4, "d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


def test_workflow_join_4b():
    """graph: D; mapper: vector; using all field in joinByKey - output: flat array """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2", "d1"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_4b", workingdir="test_join_4b")
    wf.run()

    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":20}, 23),
                ({"d1":4, "d2":10}, 14), ({"d1":4, "d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.xfail(reason="no difference in the order") # TOASK: should this work?
def test_workflow_join_4c():
    """graph: D; mapper: vector; using all field in joinByKey - output: flat array (different order than 4b) """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1", "d2"],
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_4c", workingdir="test_join_4c")
    wf.run()
    #pdb.set_trace()
    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":4, "d2":10}, 14),
                ({"d1":3, "d2":20}, 23), ({"d1":4, "d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


def test_workflow_join_4d():
    """graph: D; mapper: vector; using join=True - output: flat array (the same as 4c) """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), join=True,
              name="nD", plugin="mp")

    wf = Workflow(nodes=[nD], name="workflow_4d", workingdir="test_join_4d")
    wf.run()

    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":20}, 23),
                      ({"d1":4, "d2":10}, 14), ({"d1":4, "d2":20}, 24)])] # the same order as 4b

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]