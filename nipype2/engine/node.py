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
import re, os, time
import numpy as np
import networkx as nx
import itertools

from . import state

import pdb


class FakeNode(object):
    def __init__(self):
        self.a = 3
    def print(self):
        print("FAKE NODE!!!!!")


class Node(object):
    """Defines common attributes and functions for workflows and nodes."""

    #dj: can mapper be None??
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
        # dj TODO
        self.config = None
        self._verify_name(name)
        self.name = name
        # dj TODO: don't use it for now
        # for compatibility with node expansion using iterables
        self._id = self.name
        self._hierarchy = None
        self.plugin = plugin
        # tmp?
        self._input_order_map = {}
        self._history = {} #tracking which node should give input
        self.sufficient = True
        self._result = {}
        self._out_nm = []
        self.needed_outputs = []
        self._send_outputs = []



    @property
    def result(self):
        if self._result:
            return self._result
        else:
            raise Exception("can't find results...")
            #cwd = self.output_dir()
            # dj TODO: no self._load_resultfile
            #result, _, _ = self._load_resultfile(cwd)
            #return result

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


    # dj TODO: tmpPL czy to powinno byc  w node, czy tez przeniesieone?
    def run_interface_el(self, i, ind):
        """ running interface one element generated from node_state."""
        print("W RUN INTERFACE EL", self.name, i, ind)
        inputs_dict = self.node_states_inputs.state_values(ind)
        state_dict = self.node_states.state_values(ind)
        self._interface.run(inputs_dict)
        output = self._interface.output
        # TOTHINK: I added savings, should I both return and save?
        #TODO: specify better the name of directory
        print("W RUN INTERFACE EL, before fout",state_dict, output)
        self._out_nm = [] #TODO: should be somewhere else
        for key_out in list(output.keys()):
            self._out_nm.append(key_out)
            os.makedirs(os.path.join(self.nodedir, key_out), exist_ok=True)
            with open(os.path.join(self.nodedir, key_out, "output.txt"), "w") as fout:
                fout.write(str(output[key_out]))
        return i, state_dict, output


    def preparing_node(self):
        # adding directory (should have workflowdir already)
        self.nodedir = os.path.join(self.wfdir, self.fullname)
        os.makedirs(self.nodedir, exist_ok=True)

        #pdb.set_trace()
        self.node_states = state.State(state_inputs=self._state_inputs, mapper=self._state_mapper)
        #self._ready = []
        #for (i, ind) in enumerate(itertools.product(*self.node_states._all_elements)):
        #    dict_inp = {}
        #    for key in (self.node_states.state_inputs.keys()):
        #        if key in self._inputs:
        #            dict_inp[key] = True
        #        elif key in self._history:
        #            dict_inp[key] = None
        #        else:
        #            raise Exception("don't know how to get all inputs, do something!")
        #    self._ready.append([ind, dict_inp])
        #pdb.set_trace()
        pass

