from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

from copy import deepcopy
import re, os
import multiprocessing as mp
import numpy as np
import itertools
import networkx as nx
import pdb

from . import state, node
from .. import config, logging

from .submiter import Submiter

logger = logging.getLogger('workflow')

class Workflow(object):
    def __init__(self, workingdir, nodes=None, **kwargs):
        self.graph = nx.DiGraph()
        if nodes:
            self._nodes = nodes
            self.graph.add_nodes_from(nodes)
        else:
            self._nodes = []
        self.connected_var = {}
        for nn in self._nodes:
            self.connected_var[nn] = {}
        logger.debug('Initialize workflow')
        self.workingdir = workingdir


    @property
    def nodes(self):
        return self._nodes


    def add_nodes(self, nodes):
        """adding nodes without defining connections"""
        self._nodes += nodes
        self.graph.add_nodes_from(nodes)
        for nn in nodes:
            self.connected_var[nn] = {}


    def connect(self, from_node, from_socket, to_node, to_socket):
        self.graph.add_edges_from([(from_node, to_node)])
        if not to_node in self.nodes:
            self.add_nodes(to_node)
        self.connected_var[to_node][to_socket] = (from_node, from_socket)
        logger.debug('connecting {} and {}'.format(from_node, to_node))


    def _preparing(self):  # dj: this should be pefore running, so no results available
        self.graph_sorted = list(nx.topological_sort(self.graph))
        logger.debug('the sorted graph is: {}'.format(self.graph_sorted))
        #pdb.set_trace()
        for nn in self.graph_sorted:
            nn.wfdir = self.workingdir
            try:
                for inp, (out_node, out_var) in self.connected_var[nn].items():
                    nn.sufficient = False #it has some history (doesnt have to be in the loop)
                    nn._state_inputs.update(out_node._state_inputs)
                    for key in list(out_node._state_inputs.keys()):
                        nn._history[key] = (out_node, inp) # not sure if i will need inp
                    nn.needed_outputs.append((out_node, out_var, inp))
                    out_node._send_outputs.append((out_var, nn, inp))
#TMP: no mapper for now
                    # #pdb.set_trace()
                    # #if there is no mapper provided, i'm assuming that mapper is taken from the previous node
                    # if not nn._state_mapper or nn._state_mapper == out_node._state_mapper:
                    #     #pdb.set_trace()
                    #     nn._state_mapper = out_node._state_mapper
                    #     nn._mapper = inp
                    # # when the mapper from previous node is used in the current node (it has to be the same syntax)
                    # elif out_node._state_mapper in nn._state_mapper:  # _state_mapper or _mapper?? TODO
                    #     #dj: if I use the syntax with state_inp name than I don't have to change the mapper...
                    #     pass
                    # #TODO: implement inner mapper
                    # elif inp in nn._state_mapper:
                    #     raise Exception("{} can be in the mapper only together with {}, i.e. {})".format(inp, out[1],
                    #                                                                                     [out[1], inp]))

            except(KeyError):
                # tmp: we don't care about nn that are not in self.connected_var
                pass

            nn.preparing_node() #DJ temp: to nie run teraz robi tylko ustala jakies inputy
            #pdb.set_trace()
            pass


    def run(self, monitor_consumption=True):  # dj TODO: monitor consumption not used
        self._preparing()
        pdb.set_trace()
        sub = Submiter(self.graph_sorted, plugin="mp") #plugin has to be in the init
        sub.run_workflow()
        #pdb.set_trace()
        #pass
