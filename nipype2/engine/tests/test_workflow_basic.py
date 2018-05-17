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
    #pdb.set_trace()
    print("A Before Waiting")
    time.sleep(5)
    print("A After Waiting")
    return a**2

def funB(a):
    print("B Before Waiting")
    time.sleep(8)
    print("B After Waiting")
    return a+2

def funC(c):
    return 10 * c

def funD(a, b):
    return a + b

def funE(e1, e2):
    print("E Before Waiting")
    time.sleep(8)
    print("E After Waiting")
    return e1 * e2

def funF():
    return 0

Plugin_List = ["serial", "mp", "cf", "dask"]

@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_basic_1(plugin):
    """graph: A, B"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB")

    wf = Workflow(nodes=[nA, nB], name="workflow_1", workingdir="{}_test_1".format(plugin),
                  plugin=plugin)

    t0 = time.time()
    wf.run()
    #while not all(x.done() for x in wf.sub.worker.client.futures.values()):
    #    pdb.set_trace()
    #    time.sleep(1)

    print("time: {}".format(time.time()-t0))
    #pdb.set_trace()
    assert nA.result["out"][0][0] == {"nA-a":5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"nB-a": 15}
    assert nB.result["out"][0][1] == 17


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_basic_2(plugin):
    """graph: A -> C, B"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC")

    wf = Workflow(nodes=[nA, nB, nC], name="workflow_2",
                  workingdir="{}_test_2".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nC, "c")
    wf.run()

    assert nA.result["out"][0][0] == {"nA-a":5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"nB-a": 15}
    assert nB.result["out"][0][1] == 17

    assert nC.result["out"][0][0] == {"nA-a":5}
    assert nC.result["out"][0][1] == 250


@pytest.mark.parametrize("plugin", Plugin_List)
def test_workflow_basic_3(plugin):
    """graph: A -> C, A -> D,  B -> D, C -> E, D -> E, F"""
    nA = Node(inputs={"a": 5},
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nB = Node(inputs={"a": 15},
              interface=Function_Interface(funB, ["out"]),
              name="nB")
    nC = Node(interface=Function_Interface(funC, ["out"]),
              name="nC")
    nD = Node(interface=Function_Interface(funD, ["out"]),
              name="nD")
    nE = Node(interface=Function_Interface(funE, ["out"]),
              name="nE")
    nF = Node(interface=Function_Interface(funF, ["out"]),
              name="nF")



    wf = Workflow(nodes=[nA, nB, nC, nD, nE, nF], name="workflow_3",
                  workingdir="{}_test_3".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nC, "c")
    wf.connect(nA, "out", nD, "a")
    wf.connect(nB, "out", nD, "b")
    wf.connect(nC, "out", nE, "e1")
    wf.connect(nD, "out", nE, "e2")
    wf.run()

    assert nA.result["out"][0][0] == {"nA-a": 5}
    assert nA.result["out"][0][1] == 25

    assert nB.result["out"][0][0] == {"nB-a": 15}
    assert nB.result["out"][0][1] == 17

    assert nC.result["out"][0][0] == {"nA-a": 5}
    assert nC.result["out"][0][1] == 250

    assert nD.result["out"][0][0] == {"nA-a": 5, "nB-a": 15}
    assert nD.result["out"][0][1] == 42

    assert nE.result["out"][0][0] == {"nA-a": 5, "nB-a": 15}
    assert nE.result["out"][0][1] == 10500

    assert nF.result["out"][0][0] == {}
    assert nF.result["out"][0][1] == 0