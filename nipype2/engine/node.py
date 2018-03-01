#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

from copy import deepcopy
import re, os, time, pdb
import numpy as np
import networkx as nx
import itertools

from . import state

from .. import config, logging
logger = logging.getLogger('workflow')


class FakeNode(object):
    def __init__(self):
        self.a = 3
    def print(self):
        print("FAKE NODE!!!!!")


class Node(object):
    """Defines common attributes and functions for workflows and nodes."""

    def __init__(self, interface, name, mapper=None, reducer=None, reducer_interface=None,
                 inputs=None, base_dir=None, plugin="mp"):
        """ Initialize base parameters of a workflow or node

        Parameters
        ----------
        interface : Interface (mandatory)
            node specific interface
        inputs: dictionary
            inputs fields
        mapper: string, tuple (for scalar) or list (for outer product)
            mapper used with the interface
        reducer: string
            field used to group results
        reducer_interface: Interface
            interface used to reduce results
        name : string (mandatory)
            Name of this node. Name must be alphanumeric and not contain any
            special characters (e.g., '.', '@').
        base_dir : string
            base output directory (will be hashed before creations)
            default=None, which results in the use of mkdtemp

        """
        self._mapper = mapper
        # contains variables from the state (original) variables
        self._state_mapper = self._mapper
        self._reducer = reducer
        self._reducer_interface = reducer_interface
        if inputs:
            self._inputs = inputs
            # extra input dictionary needed to save values of state inputs
            self._state_inputs = self._inputs.copy()
        else:
            self._inputs = {}
            self._state_inputs = {}

        self._interface = interface
        self.base_dir = base_dir
        self.config = None
        self._verify_name(name)
        self.name = name
        # for compatibility with node expansion using iterables
        self._id = self.name
        self._hierarchy = None
        self.plugin = plugin
        self._input_order_map = {}
        self.sufficient = True
        self._result = {}
        self.needed_outputs = []
        self.sending_output = [] # what should be send to another nodes
        if self._reducer is None:
            self._out_nm = self._interface._output_nm
        else:
            raise Exception("have to finish...")
        logger.debug('Initialize Node {}'.format(name))
        self._global_done = False # if all tasks are done (if mapper present, I'm checking for all state elements)


    @property
    def global_done(self):
        # once _global_done os True, this should not change
        if self._global_done:
            return self._global_done
        else:
            return self._check_all_results()


    def _check_all_results(self):
        # checking if all files that should be created are present
        # TODO: if reducer is present, that should be changed
        for ind in itertools.product(*self.node_states._all_elements):
            state_dict = self.node_states.state_values(ind)
            dir_nm_el = "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())])
            os.makedirs(os.path.join(self.nodedir, dir_nm_el), exist_ok=True)
            for key_out in self._interface._output_nm:
                if not os.path.isfile(os.path.join(self.nodedir, dir_nm_el, key_out+".txt")):
                    return False
        self._global_done = True
        return True


    @property
    def result(self):
        if self._result:
            return self._result
        else:
            raise Exception("can't find results...")


    @property
    def inputs(self):
        """Return the inputs of the underlying interface"""
        #return self._interface.inputs
        # dj: temporary will use self._inputs
        return self._inputs

    @inputs.setter
    def inputs(self, inputs):
        self._inputs = inputs
        self._state_inputs = self._inputs.copy()


    @property
    def outputs(self):
        """Return the output fields of the underlying interface"""
        return self._interface._outputs()

    @property
    def interface(self):
        """Return the underlying interface object"""
        return self._interface

    @property
    def fullname(self):
        fullname = self.name
        if self._hierarchy:
            fullname = self._hierarchy + '.' + self.name
        return fullname


    def _verify_name(self, name):
        valid_name = bool(re.match('^[\w-]+$', name))
        if not valid_name:
            raise ValueError('[Workflow|Node] name \'%s\' contains'
                                  ' special characters' % name)


    def __repr__(self):
        if self._hierarchy:
            return '.'.join((self._hierarchy, self._id))
        else:
            return '{}'.format(self._id)


    def run_interface_el(self, i, ind):
        """ running interface one element generated from node_state."""
        logger.debug("Run interface el, name={}, i={}, in={}".format(self.name, i, ind))
        inputs_dict = self.node_states_inputs.state_values(ind)
        state_dict = self.node_states.state_values(ind)
        # reading extra inputs that come from previous nodes
        for (from_node, from_socket, to_socket) in self.needed_outputs:
            dir_nm_el_from = "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())
                                       if i in list(from_node._state_inputs.keys())])
            file_from = os.path.join(from_node.nodedir, dir_nm_el_from, from_socket+".txt")
            with open(file_from) as f:
                #print("RUN INTERFACE LOOP FILE", f.readline())
                inputs_dict[to_socket] = eval(f.readline())

        self._interface.run(inputs_dict)
        output = self._interface.output
        dir_nm_el = "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())])
        os.makedirs(os.path.join(self.nodedir, dir_nm_el), exist_ok=True)
        for key_out in list(output.keys()):
            with open(os.path.join(self.nodedir, dir_nm_el, key_out+".txt"), "w") as fout:
                fout.write(str(output[key_out]))


    def preparing_node(self):
        # adding directory (should have workflowdir already)
        self.nodedir = os.path.join(self.wfdir, self.fullname)
        os.makedirs(self.nodedir, exist_ok=True)

        self.node_states = state.State(state_inputs=self._state_inputs, mapper=self._state_mapper)


