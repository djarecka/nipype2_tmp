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
    def __init__(self, workingdir, plugin="mp", nodes=None, **kwargs):
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
        self.plugin = plugin


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
        from_node.sending_output.append((from_socket, to_node, to_socket))
        logger.debug('connecting {} and {}'.format(from_node, to_node))


    def _preparing(self):
        self.graph_sorted = list(nx.topological_sort(self.graph))
        logger.debug('the sorted graph is: {}'.format(self.graph_sorted))
        #pdb.set_trace()
        for nn in self.graph_sorted:
            nn.wfdir = self.workingdir
            try:
                for inp, (out_node, out_var) in self.connected_var[nn].items():
                    nn.sufficient = False #it has some history (doesnt have to be in the loop)
                    nn.state_inputs.update(out_node.state_inputs)
                    nn.needed_outputs.append((out_node, out_var, inp))
                    #if there is no mapper provided, i'm assuming that mapper is taken from the previous node
                    if (not nn.state_mapper or nn.state_mapper == out_node.state_mapper) and out_node.state_mapper:
                        nn.state_mapper = out_node.state_mapper
                        #nn._mapper = inp #not used
                    elif not out_node.state_mapper: # we shouldn't change anything
                        pass
                    # when the mapper from previous node is used in the current node (it has to be the same syntax)
                    elif nn.state_mapper and out_node.state_mapper in nn.state_mapper:  # state_mapper or _mapper?? TODO
                        #dj: if I use the syntax with state_inp name than I don't have to change the mapper...
                        #if type(nn._mapper) is tuple:
                        #    nn._mapper = tuple([inp if x == out_node.state_mapper else x for x in list(nn._mapper)])
                        # TODO: not sure if I'll have to implement more
                        pass

                    #TODO: implement inner mapper
                    # TODO: if nn.state_mapper is a string and inp can be a letter that exists in nn.state_mapper
                    elif  nn.state_mapper and inp in nn.state_mapper:
                        raise Exception("{} can be in the mapper only together with {}, i.e. {})".format(inp, out[1],
                                                                                                        [out[1], inp]))
                    else:
                        raise Exception("worflow._preparing: should I implement something more?")
                pass
            except(KeyError):
                # tmp: we don't care about nn that are not in self.connected_var
                pass

            nn.preparing_node()


    def run(self):
        self._preparing()
        sub = Submiter(self.graph_sorted, plugin=self.plugin)
        sub.run_workflow()
        sub.close()
