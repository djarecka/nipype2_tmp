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


Plugin_List = ["serial", "mp"]

@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_1(plugin):
    """graph: A"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", joinByKey=["a"],
              interface=Function_Interface(funA, ["out"]),
              name="nA")

    wf = Workflow(nodes=[nA], name="workflow_1",
                  workingdir="{}_test_join_1".format(plugin), plugin=plugin)
    wf.run()
    #pdb.set_trace()

    expected = [({}, [({"nA-a": 3}, 9), ({"nA-a": 4}, 16), ({"nA-a": 5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_1a(plugin):
    """graph: A; should be the same as 1"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", join=True,
              interface=Function_Interface(funA, ["out"]),
              name="nA")

    wf = Workflow(nodes=[nA], name="workflow_1a",
                  workingdir="{}_test_join_1a".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"nA-a":3}, 9), ({"nA-a":4}, 16), ({"nA-a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_2(plugin):
    """graph: D; scalar mapper"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2",
                  workingdir="{}_test_join_2".format(plugin), plugin=plugin)
    wf.run()

    expected = [
        ({"nD-d1":3}, [({"nD-d1":3, "nD-d2":10}, 13),({"nD-d1":3, "nD-d2":40}, 43)]), # TOASK: should this be merged
        ({"nD-d1":4}, [({"nD-d1":4, "nD-d2":20}, 24)]),
        ({"nD-d1":5}, [({"nD-d1":5, "nD-d2":30}, 35)])
        ]
    #pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_2a(plugin):
    """graph: D; scalar mapper; it will be exactly the same output as 2"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2a",
                  workingdir="{}_test_join_2a".format(plugin), plugin=plugin)
    wf.run()

    expected = [
            ({"nD-d2":10}, [({"nD-d1":3, "nD-d2":10}, 13)]),
            ({"nD-d2":20}, [({"nD-d1":4, "nD-d2":20}, 24)]),
            ({"nD-d2":30}, [({"nD-d1":5, "nD-d2":30}, 35)]),
            ({"nD-d2":40}, [({"nD-d1":3, "nD-d2":40}, 43)])
        ]
    #pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_2b(plugin):
    """graph: D; scalar mapper; it will be exactly the same output as 2"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]), join=True,
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2b",
                  workingdir="{}_test_join_2b".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":3, "nD-d2":40}, 43), #the order like in 2
                      ({"nD-d1":4, "nD-d2":20}, 24), ({"nD-d1":5, "nD-d2":30}, 35)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
@pytest.mark.xfail(reason="join TODO")
def test_workflow_join_3(plugin):
    """graph: A -> D, joinByKey in the second key"""
    nA = Node(inputs={"a": np.array([3, 4, 3])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nD = Node(inputs={"d1": np.array([10, 20, 30])},
              mapper=("nA-a", "d1"), joinByKey=["nA-a"],
              interface=Function_Interface(funD, ["out"]),
              name="nD")

    wf = Workflow(nodes=[nA, nD], name="workflow_3",
                  workingdir="{}_test_join_3".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    expected = [
        ({"nD-d1":10}, [({"nA-a":3, "nD-d1": 10}, 19)]),
        ({"nD-d1":20}, [({"nA-a":4, "nD-d1":20}, 36)]),
        ({"nD-d1":30}, [({"nA-a":3, "nD-d1": 30}, 39)])
            ]
    pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_4(plugin):
    """graph: D; mapper: vector """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_4",
                  workingdir="{}_test_join_4".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"nD-d1":3}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":3, "nD-d2":20}, 23)]),
                ({"nD-d1":4}, [({"nD-d1":4, "nD-d2":10}, 14), ({"nD-d1":4, "nD-d2":20}, 24)])]
    pdb.set_trace()
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_4a(plugin):
    """graph: D; mapper: vector; this is 'transpose' to 4 """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_4a",
                  workingdir="{}_test_join_4a".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"nD-d2":10}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":4, "nD-d2":10}, 14)]),
                ({"nD-d2":20}, [({"nD-d1":3, "nD-d2":20}, 23), ({"nD-d1":4, "nD-d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_4b(plugin):
    """graph: D; mapper: vector; using all field in joinByKey - output: flat array """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d2", "d1"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_4b",
                  workingdir="{}_test_join_4b".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":3, "nD-d2":20}, 23),
                ({"nD-d1":4, "nD-d2":10}, 14), ({"nD-d1":4, "nD-d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.xfail(reason="no difference in the order") # TOASK: should this work?
@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_4c(plugin):
    """graph: D; mapper: vector; using all field in joinByKey - output: flat array (different order than 4b) """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), joinByKey=["d1", "d2"],
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_4c",
                  workingdir="{}_test_join_4c".format(plugin), plugin=plugin)
    wf.run()
    #pdb.set_trace()
    expected = [({}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":4, "nD-d2":10}, 14),
                ({"nD-d1":3, "nD-d2":20}, 23), ({"nD-d1":4, "nD-d2":20}, 24)])]

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_join_4d(plugin):
    """graph: D; mapper: vector; using join=True - output: flat array (the same as 4c) """
    nD = Node(inputs={"d1": np.array([3, 4]), "d2": np.array([10, 20])}, mapper=["d1", "d2"],
              interface=Function_Interface(funD, ["out"]), join=True,
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_4d",
                  workingdir="{}_test_join_4d".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"nD-d1":3, "nD-d2":10}, 13), ({"nD-d1":3, "nD-d2":20}, 23),
                      ({"nD-d1":4, "nD-d2":10}, 14), ({"nD-d1":4, "nD-d2":20}, 24)])] # the same order as 4b

    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nD.result["out"][i][1][j][0] == res_el[0]
            assert nD.result["out"][i][1][j][1] == res_el[1]