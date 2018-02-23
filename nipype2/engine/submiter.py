from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

import os, pdb, time
import itertools
import queue

from .workers import MpWorker
from .state import State

class Submiter(object):
    def __init__(self, graph, plugin):
        self.graph = graph
        self.plugin = plugin
        self.node_line = []
        #self.done_wf = []
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

                    if os.path.isfile(os.path.join(out_node.nodedir, out_var, "output.txt")):
                        with open(os.path.join(out_node.nodedir, out_var, "output.txt")) as f:
                            node.inputs.update({inp: eval(f.readline())})
                        node.needed_outputs.remove((out_node, out_var, inp))
                if not node.needed_outputs:
                    self.node_line.remove((node, i_n))
                    node.sufficient = True
                    self.submit_work(node, i_n)
                print("INP, NODE end", node, node.inputs)
            print("NODE LINE before sleep", self.node_line)
            time.sleep(3)

        time.sleep(5)
        # final reading of the results
        for (i_n, node) in enumerate(self.graph):
            #print("OUT NM", node._out_nm) #TODO i don't know why I dont see it
            for key_out in ["out"]: #TODO: node._out_nm:
                with open(os.path.join(node.nodedir, key_out, "output.txt")) as fout:
                    node._result[key_out] = eval(fout.readline())
            #print("AFTER RESULT", node, node._result)


    def submit_work(self, node, i_n):
        print("SUBMIT WORKER, needed out", node, node.needed_outputs)
        if node.sufficient: #doesnt have to wait for anyone (probably should be generalized)
            print("SELF SUFF", node)
            node.node_states_inputs = State(state_inputs=node._inputs, mapper=node._mapper,
                                                    inp_ord_map=node._input_order_map)



            for (i, ind) in enumerate(itertools.product(*node.node_states._all_elements)):
                self.worker.run_el(node.run_interface_el, (i, ind))


