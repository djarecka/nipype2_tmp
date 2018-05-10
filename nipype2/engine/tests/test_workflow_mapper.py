import pytest, pdb
import numpy as np
import time, os

from ..node import Node
from ..workflow import Workflow
from ..auxiliary import Function_Interface

from ... import config, logging
config.enable_debug_mode()
logging.update_logging(config)
logger = logging.getLogger('workflow')

def funA(a):
    print("A Before Waiting")
    time.sleep(5)
    print("A After Waiting")
    if a==5:
        time.sleep(10)
        print("A after extra waiting")
    return a**2

def funB(a):
    return a+2

def funC(c):
    return 10 * c

def funD(d1, d2):
    print("IM IN D", d1, d2)
    return d1 + d2

def funE(e1, e2):
    time.sleep(10)
    return e1 * e2

def funF():
    return 0

Plugin_List = ["serial", "mp"]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_1(plugin):
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")

    wf = Workflow(nodes=[nA], name="workflow_1",
                  workingdir="{}_test_mapper_1".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"nA-a":3}, 9), ({"nA-a":4}, 16), ({"nA-a":5}, 25),
                ({"nA-a": 6}, 36), ({"nA-a": 7}, 49), ({"nA-a": 8}, 64)]
    logger.debug("TEST, result['out']={}".format(nA.result["out"]))
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_1a(plugin):
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5]), "d2": np.array([10, 20, 30])}, mapper=("d1", "d2"),
              interface=Function_Interface(funD, ["out"]),
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_1a",
                  workingdir="{}_test_mapper_1a".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"nD-d1":3, "nD-d2": 10}, 13), ({"nD-d1":4, "nD-d2":20}, 24), ({"nD-d1":5, "nD-d2":30}, 35)]
    logger.debug("TEST, result['out']={}".format(nD.result["out"]))
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_2(plugin):
    """graph: A, B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB")

    wf = Workflow(nodes=[nA, nB], name="workflow_2",
                  workingdir="{}_test_mapper_2".format(plugin), plugin=plugin)
    wf.run()

    logger.debug("TEST, nA.result['out']={}".format(nA.result["out"]))
    logger.debug("TEST, nB.result['out']={}".format(nB.result["out"]))
    expected = [({"nA-a":3}, 9), ({"nA-a":4}, 16), ({"nA-a":5}, 25),
                ({"nA-a": 6}, 36), ({"nA-a": 7}, 49), ({"nA-a": 8}, 64)]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    assert nB.result["out"][0][0] == {"nB-a": 15}
    assert nB.result["out"][0][1] == 17


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_3(plugin):
    """graph: A -> B"""
    nA = Node(inputs={"a": np.array([3, 4, 5, 6, 7, 8])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC", mapper="nA-a")

    wf = Workflow(nodes=[nA, nC], name="workflow_3",
                  workingdir="{}_test_mapper_3".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nC, "c")
    wf.run()

    logger.debug("TEST, nA.result['out']={}".format(nA.result["out"]))
    expected_A = [({"nA-a":3}, 9), ({"nA-a":4}, 16), ({"nA-a":5}, 25),
                 ({"nA-a": 6}, 36), ({"nA-a": 7}, 49), ({"nA-a": 8}, 64)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    logger.debug("TEST, nC.result['out']={}".format(nC.result["out"]))
    expected_C = [({"nA-a": 3}, 90), ({"nA-a": 4}, 160), ({"nA-a": 5}, 250),
                  ({"nA-a": 6}, 360), ({"nA-a": 7}, 490), ({"nA-a": 8}, 640)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_4(plugin):
    """graph: A -> D"""
    nA = Node(inputs={"a": np.array([3, 4, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nD = Node(inputs={"d1": np.array([10, 20, 30])}, mapper=("nA-a", "d1"),
              interface=Function_Interface(funD, ["out"]),
              name="nD")

    wf = Workflow(nodes=[nA, nD], name="workflow_4",
                  workingdir="{}_test_mapper_4".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    logger.debug("TEST, nA.result['out']={}".format(nA.result["out"]))
    expected_A = [({"nA-a":3}, 9), ({"nA-a":4}, 16), ({"nA-a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    logger.debug("TEST, nD.result['out']={}".format(nD.result["out"]))
    expected_D = [({"nA-a":3, "nD-d1":10}, 19), ({"nA-a":4, "nD-d1":20}, 36), ({"nA-a":5, "nD-d1":30}, 55)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_5(plugin):
    """graph: A -> C, A -> D,  B -> D, F"""
    nA = Node(inputs={"a": np.array([3, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": np.array([10, 20])}, mapper="a",
              interface=Function_Interface(funB, ["out"]),
              name="nB")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD", mapper=("nA-a", "nB-a"))
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF")

    wf = Workflow(nodes=[nA, nB, nC, nD, nF], name="workflow_5",
                  workingdir="{}_test_mapper_5".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "d1")
    wf.connect(nB, "out", nD, "d2")
    wf.run()

    expected_A = [({"nA-a":3}, 9), ({"nA-a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_B = [({"nB-a":10}, 12), ({"nB-a":20}, 22)]
    for i, res in enumerate(expected_B):
        assert nB.result["out"][i][0] == res[0]
        assert nB.result["out"][i][1] == res[1]

    expected_C = [({"nA-a": 3}, 90), ({"nA-a": 5}, 250)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]


    expected_D = [({"nA-a":3, "nB-a":10}, 21), ({"nA-a":5, "nB-a":20}, 47)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_mapper_6(plugin):
    """graph: A -> C, A -> D,  B -> D, C -> E, D -> E, F"""
    nA = Node(inputs={"a": np.array([3, 5])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": np.array([10, 20])}, mapper="a",
              interface=Function_Interface(funB, ["out"]),
              name="nB")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD", mapper=("nA-a", "nB-a"))
    nE = Node(interface=Function_Interface(funE, ["out"]),
              name="nE", mapper=("nA-a", "nB-a"))
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF")

    wf = Workflow(nodes=[nA, nB, nC, nD, nE, nF], name="workflow_6",
                  workingdir="{}_test_mapper_6".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "d1")
    wf.connect(nB, "out", nD, "d2")
    wf.connect(nC, "out", nE, "e1")
    wf.connect(nD, "out", nE, "e2")
    wf.run()

    expected_A = [({"nA-a":3}, 9), ({"nA-a":5}, 25)]
    for i, res in enumerate(expected_A):
        assert nA.result["out"][i][0] == res[0]
        assert nA.result["out"][i][1] == res[1]

    expected_B = [({"nB-a":10}, 12), ({"nB-a":20}, 22)]
    for i, res in enumerate(expected_B):
        assert nB.result["out"][i][0] == res[0]
        assert nB.result["out"][i][1] == res[1]

    expected_C = [({"nA-a": 3}, 90), ({"nA-a": 5}, 250)]
    for i, res in enumerate(expected_C):
        assert nC.result["out"][i][0] == res[0]
        assert nC.result["out"][i][1] == res[1]

    expected_D = [({"nA-a":3, "nB-a":10}, 21), ({"nA-a":5, "nB-a":20}, 47)]
    for i, res in enumerate(expected_D):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_E = [({"nA-a":3, "nB-a":10}, 1890), ({"nA-a":5, "nB-a":20}, 11750)]
    for i, res in enumerate(expected_E):
        assert nE.result["out"][i][0] == res[0]
        assert nE.result["out"][i][1] == res[1]

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0