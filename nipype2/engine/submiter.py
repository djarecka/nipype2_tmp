from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

import os, pdb, time, glob
import itertools, collections
import queue

from .workers import MpWorker
from .state import State

class Submiter(object):
    def __init__(self, graph, plugin):
        self.graph = graph
        self.plugin = plugin
        self.node_line = []
        if self.plugin == "mp":
            self.worker = MpWorker()



    def run_workflow(self):
        for (i_n, node) in enumerate(self.graph):
            # checking if a node has all input (doesnt have to wait for others)
            if node.sufficient:
                self.submit_work(node, i_n)
            # if its not, its been added to a line
            else:
                self.node_line.append((node, i_n))

        time.sleep(5)

        while self.node_line:
            print("NODE LINE", self.node_line)
            for i, (node, i_n) in enumerate(self.node_line):
                for (out_node, out_var, inp) in node.needed_outputs:
                    # TODO this works only because there is no mapper!
                    try:
                        file_output = [name for name in glob.glob("{}/*/{}.txt".format(out_node.nodedir, out_var))][0]
                    except(IndexError):
                        file_output = None
                    if file_output and os.path.isfile(file_output):
                        with open(file_output) as f:
                            node.inputs.update({inp: eval(f.readline())})
                        node.needed_outputs.remove((out_node, out_var, inp))
                if not node.needed_outputs:
                    self.node_line.remove((node, i_n))
                    node.sufficient = True
                    self.submit_work(node, i_n)
            print("NODE LINE before sleep", self.node_line)
            time.sleep(3)

        time.sleep(5)

        # final reading of the results, this will be removed in the final version (TODO)
        # combining all results from specifics nodes together (for all state elements)
        for (i_n, node) in enumerate(self.graph):
            for key_out in node._out_nm:
                node._result[key_out] = []
                if node._inputs:
                    files = [name for name in glob.glob("{}/*/{}.txt".format(node.nodedir, key_out))]
                    for file in files:
                        st_el = file.split(os.sep)[-2].split("_")
                        st_dict =  collections.OrderedDict([(el.split(".")[0], eval(el.split(".")[1]))
                                                                for el in st_el])
                        with open(file) as fout:
                            node._result[key_out].append((st_dict, eval(fout.readline())))
                else:
                    files = [name for name in glob.glob("{}/{}.txt".format(node.nodedir, key_out))]
                    with open(files[0]) as fout:
                        node._result[key_out].append(({}, eval(fout.readline())))


    def submit_work(self, node, i_n):
        print("SUBMIT WORKER, needed out", node, node.needed_outputs)
        if node.sufficient: #doesnt have to wait for anyone (probably should be generalized)
            print("SELF SUFF", node)
            node.node_states_inputs = State(state_inputs=node._inputs, mapper=node._mapper,
                                                    inp_ord_map=node._input_order_map)



            for (i, ind) in enumerate(itertools.product(*node.node_states._all_elements)):
                self.worker.run_el(node.run_interface_el, (i, ind))


