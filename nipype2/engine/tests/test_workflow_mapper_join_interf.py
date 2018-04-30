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


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_1(plugin):
    """graph: A"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", joinByKey=["a"],
              interface=Function_Interface(funA, ["out"]),
              join_fun_inp=(fun_sum, "out"),
              name="nA")

    wf = Workflow(nodes=[nA], name="workflow_1",
                  workingdir="{}_test_reducer_interf_1".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]

    expected_redu_interf = [({}, 50)]
    for i, res in enumerate(expected_redu_interf):
        assert nA.result_join_interf["red_out"][i][0] == res[0]
        assert nA.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_1a(plugin):
    """graph: A"""
    nA = Node(inputs={"a": np.array([3, 4, 5])},
              mapper="a", join=True,
              interface=Function_Interface(funA, ["out"]),
              join_fun_inp=(fun_sum, "out"),
              name="nA")

    wf = Workflow(nodes=[nA], name="workflow_1a",
                  workingdir="{}_test_reducer_interf_1a".format(plugin), plugin=plugin)
    wf.run()

    expected = [({}, [({"a":3}, 9), ({"a":4}, 16), ({"a":5}, 25)])]
    for i, res in enumerate(expected):
        assert nA.result["out"][i][0] == res[0]
        for j, res_el in enumerate(res[1]):
            assert nA.result["out"][i][1][j][0] == res_el[0]
            assert nA.result["out"][i][1][j][1] == res_el[1]

    expected_redu_interf = [({}, 50)]
    for i, res in enumerate(expected_redu_interf):
        assert nA.result_join_interf["red_out"][i][0] == res[0]
        assert nA.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_2(plugin):
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])},
              mapper=("d1", "d2"), interface=Function_Interface(funD, ["out"]),
              joinByKey=["d2"], join_fun_inp=(fun_sum, "out"),
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_1a",
                  workingdir="{}_test_reducer_interf_2".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"d1":3}, [({"d1":3, "d2": 10}, 13), ({"d1":3, "d2": 40}, 43)]),
                ({"d1":4}, [({"d1":4, "d2":20}, 24)]),
                ({"d1":5}, [({"d1":5, "d2":30}, 35)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({"d1":3}, 56), ({"d1":4}, 24), ({"d1":5}, 35)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_join_interf["red_out"][i][0] == res[0]
        assert nD.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_2a(plugin):
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])},
              mapper=("d1", "d2"), interface=Function_Interface(funD, ["out"]),
              joinByKey=["d1"], join_fun_inp=(fun_sum, "out"),
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2a",
                  workingdir="{}_test_reducer_interf_2a".format(plugin), plugin=plugin)
    wf.run()

    expected = [({"d2":10}, [({"d1":3, "d2":10}, 13)]),
                ({"d2":20}, [({"d1":4, "d2":20}, 24)]),
                ({"d2":30}, [({"d1":5, "d2":30}, 35)]),
                ({"d2":40}, [({"d1":3, "d2":40}, 43)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({"d2":10}, 13), ({"d2":20}, 24), ({"d2":30}, 35), ({"d2":40}, 43)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_join_interf["red_out"][i][0] == res[0]
        assert nD.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_2b(plugin):
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])},
              mapper=("d1", "d2"), interface=Function_Interface(funD, ["out"]),
              joinByKey=["d1", "d2"], join_fun_inp=(fun_sum, "out"),
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2b",
                  workingdir="{}_test_reducer_interf_2b".format(plugin), plugin=plugin)
    wf.run()

    # TODO: not sure if the order is "correct"...
    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":40}, 43),
                      ({"d1":4, "d2":20}, 24), ({"d1":5, "d2":30}, 35)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({}, 115)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_join_interf["red_out"][i][0] == res[0]
        assert nD.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_2c(plugin):
    """graph: D"""
    nD = Node(inputs={"d1": np.array([3, 4, 5, 3]), "d2": np.array([10, 20, 30, 40])},
              mapper=("d1", "d2"), interface=Function_Interface(funD, ["out"]),
              join=True, join_fun_inp=(fun_sum, "out"),
              name="nD")

    wf = Workflow(nodes=[nD], name="workflow_2c",
                  workingdir="{}_test_reducer_interf_2c".format(plugin), plugin=plugin)
    wf.run()

    # TODO: not sure if the order is "correct"...
    expected = [({}, [({"d1":3, "d2":10}, 13), ({"d1":3, "d2":40}, 43),
                      ({"d1":4, "d2":20}, 24), ({"d1":5, "d2":30}, 35)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({}, 115)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_join_interf["red_out"][i][0] == res[0]
        assert nD.result_join_interf["red_out"][i][1] == res[1]


@pytest.mark.parametrize("plugin", ["mp", "serial"])
def test_workflow_reducer_interf_3(plugin):
    """graph: A -> D"""
    nA = Node(inputs={"a": np.array([3, 4, 3])}, mapper="a",
              interface=Function_Interface(funA, ["out"]),
              name="nA")
    nD = Node(inputs={"d1": np.array([10, 20, 30])},
              mapper=("a", "d1"), joinByKey=["d1"],
              join_fun_inp=(fun_sum, "out"),
              interface=Function_Interface(funD, ["out"]),
              name="nD")

    wf = Workflow(nodes=[nA, nD], name="workflow_4",
                  workingdir="{}_test_reducer_interf_3".format(plugin), plugin=plugin)
    wf.connect(nA, "out", nD, "d2")
    wf.run()

    expected = [({"a":3}, [({"a":3, "d1": 10}, 19), ({"a":3, "d1": 30}, 39)]),
                ({"a": 4}, [({"a":4, "d1":20}, 36)])]
    for i, res in enumerate(expected):
        assert nD.result["out"][i][0] == res[0]
        assert nD.result["out"][i][1] == res[1]

    expected_redu_interf = [({"a":3}, 58), ({"a":4}, 36)]
    for i, res in enumerate(expected_redu_interf):
        assert nD.result_join_interf["red_out"][i][0] == res[0]
        assert nD.result_join_interf["red_out"][i][1] == res[1]