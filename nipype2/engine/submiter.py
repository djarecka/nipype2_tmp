from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

import os, pdb, time
import itertools
import queue

from .workers import MpWorker, run_fun
from .state import State
from .node import Node, FakeNode

class Submiter(object):
    def __init__(self, graph, plugin):
        self.graph = graph
        self.plugin = plugin
        self.done_wf = []
        self.queue_wf = []


    def run_workflow(self):
        for (i_n, node) in enumerate(self.graph):
            #pdb.set_trace()
            self.done_wf.append([])
            self.queue_wf.append(queue.Queue())
            self.submit_work(node, i_n)
            #pdb.set_trace()
            #pass

        #dj: this doesn't work...
        #print("adding additinioanl el")
        #self.queue_wf[0].put((2, 10))
        #should be more clever callback mechanism
        for (i_n, node) in enumerate(self.graph):
            while len(self.done_wf[i_n]) < node.nr_el:
                print("waiting in runworkflow")
                time.sleep(2)
        #pdb.set_trace()
        #pass

    def submit_work(self, node, i_n):
        if self.plugin == "mp":
            worker = MpWorker(node, self.done_wf[i_n], self.queue_wf[i_n])
        #pdb.set_trace()
        if node.sufficient: #doesnt have to wait for anyone (probably should be generalized)
            print("SELF SUFF", node)
            node.node_states_inputs = State(state_inputs=node._inputs, mapper=node._mapper,
                                                    inp_ord_map=node._input_order_map)

            #not sure if input_list will be needed
            input_list = []
            for (i, ind) in enumerate(itertools.product(*node.node_states._all_elements)):
                input_list.append((i, ind))
                self.queue_wf[i_n].put((i, ind))

            node.nr_el = len(input_list)

            #worker.run()
            #pdb.set_trace()
            for ii in range(node.nr_el):
                in_el = self.queue_wf[i_n].get() #if timeout=2 it will be raised an error when the queue is empty
                worker.run_el(in_el)


            #pdb.set_trace()
            node._result = self.done_wf[i_n]

        else:
            print("NOT SELF SUFF", node)
            pdb.set_trace()
