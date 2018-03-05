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

from .. import config, logging
logger = logging.getLogger('workflow')


class Submiter(object):
    def __init__(self, graph, plugin):
        self.graph = graph
        self.plugin = plugin
        self.node_line = []
        if self.plugin == "mp":
            self.worker = MpWorker()
        logger.debug('Initialize Submitter, graph: {}'.format(graph))
        self._to_finish = list(self.graph)

    def run_workflow(self):
        for (i_n, node) in enumerate(self.graph):
            # submitting all the nodes who are self sufficient (self.graph is already sorted)
            if node.sufficient:
                self.submit_work(node)
            # if its not, its been added to a line
            else:
                break

        # all nodes that are not self sufficient will go to the line
        # (i think ordered list work well here, since it's more efficient to check within a specific order)
        self.node_line = self.graph[i_n:]

        # this parts submits nodes that are waiting to be run
        # it should stop when nothing is waiting
        while self._nodes_check():
            logger.debug("Submitter, in while, node_line: {}".format(self.node_line))
            time.sleep(3)

        # TODO(?): combining two while together
        # this part simply waiting for all "last nodes" to finish
        while self._output_check():
            logger.debug("Submitter, in while, to_finish: {}".format(self._to_finish))
            time.sleep(3)


    def _nodes_check(self):
        for to_node in self.node_line:
            ready = True
            for (from_node, from_socket, to_socket) in to_node.needed_outputs:
                if from_node.global_done:
                    try:
                        self._to_finish.remove(from_node)
                    except ValueError:
                        pass
                else:
                    ready = False
                    break
            if ready:
                self.submit_work(to_node)
                self.node_line.remove(to_node)
        return self.node_line


    def _output_check(self):
        for node in self._to_finish:
            if node.global_done:
                self._to_finish.remove(node)
        return self._to_finish


    def submit_work(self, node):
        node.node_states_inputs = State(state_inputs=node._inputs, mapper=node._mapper,
                                        inp_ord_map=node._input_order_map)
        for (i, ind) in enumerate(itertools.product(*node.node_states._all_elements)):
            logger.debug("SUBMIT WORKER, node: {}, ind: {}".format(node, ind))
            self.worker.run_el(node.run_interface_el, (i, ind))
