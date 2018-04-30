#!/usr/bin/env python
# -*- coding: utf-8 -*-
# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:

from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object

from future import standard_library
standard_library.install_aliases()

import re, os, glob
import itertools
import collections
import pdb

from . import state
from .auxiliary import Function_Interface

from .. import config, logging
logger = logging.getLogger('workflow')



class Node(object):
    """Defines common attributes and functions for workflows and nodes."""

    def __init__(self, interface, name, inputs=None, mapper=None,
                 join=False, joinByKey=None, join_fun_inp=None,
                 base_dir=None):
        """ Initialize base parameters of a workflow or node

        Parameters
        ----------
        interface : Interface (mandatory)
            node specific interface
        inputs: dictionary
            inputs fields
        mapper: string, tuple (for scalar) or list (for outer product)
            mapper used with the interface
        join: Bool
            joining all elements (after mapping) together
        joinByKey: list
            list of fields will be used for joining
        join_fun_inp: tuple
            (function used for joined output,
            output name from the interface that should go to the function)
        name : string (mandatory)
            Name of this node. Name must be alphanumeric and not contain any
            special characters (e.g., '.', '@').
        base_dir : string
            base output directory (will be hashed before creations)
            default=None, which results in the use of mkdtemp

        """
        #self._mapper = mapper # not used?
        # contains variables from the state (original) variables
        self.state_mapper = mapper
        if join and joinByKey:
            raise Exception("you cant have join and joinByKey at the same time")
        self._join = join
        self._joinByKey = joinByKey
        if join_fun_inp and not (self._join or self._joinByKey):
            raise Exception("you have to have join or joinByKey to use join_interface")
        elif join_fun_inp:
            self._join_interface_input = join_fun_inp[1]
            self._join_interface = Function_Interface(join_fun_inp[0], ["red_{}".format(self._join_interface_input)])
        else:
            self._join_interface = None

        if inputs:
            self._inputs = inputs
            # extra input dictionary needed to save values of state inputs
            self.state_inputs = self._inputs.copy()
        else:
            self._inputs = {}
            self.state_inputs = {}

        self._interface = interface
        self.base_dir = base_dir
        self.config = None
        self._verify_name(name)
        self.name = name
        # for compatibility with node expansion using iterables
        self._id = self.name
        self._hierarchy = None
        self.sufficient = True
        self._result = {}
        self._result_join_interf = {}
        self.needed_outputs = []
        self.sending_output = [] # what should be send to another nodes
        # TODO: should I change it for join
        self._out_nm = self._interface._output_nm
        logger.debug('Initialize Node {}'.format(name))
        self._global_done = False # if all tasks are done (if mapper present, I'm checking for all state elements)
        self._global_done_join = False  # if reduction function is done


    @property
    def global_done(self):
        # once _global_done os True, this should not change
        logger.debug('global_done {}'.format(self._global_done))
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
            if self._joinByKey:
                dir_red = "join_" + "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items()) if i not in self._joinByKey])
                dir_nm_el = os.path.join(dir_red, dir_nm_el)
            elif self._join:
                dir_nm_el = os.path.join("join_", dir_nm_el)
            for key_out in self._out_nm:
                if not os.path.isfile(os.path.join(self.nodedir, dir_nm_el, key_out+".txt")):
                    return False
        self._global_done = True
        return True


    @property
    def global_done_join(self):
        # once _global_done_join is True, this should not change
        logger.debug('global_done {}'.format(self._global_done_join))
        if self._global_done_join:
            return self._global_done_join
        else:
            return self._check_all_results_join_interf()


    def _check_all_results_join_interf(self):
        # checking if all files that should be created are present
        key_red_interf = self._join_interface_input
        for (state_redu, res_redu) in self.result[key_red_interf]:
            dir_red = "join_" + "_".join(["{}.{}".format(i, j) for i, j in list(state_redu.items())])
            if not os.path.isfile(os.path.join(self.nodedir, dir_red, "red_" + key_red_interf + ".txt")):
                return False
        return True


    @property
    def result(self):
        if not self._result:
            if self._joinByKey or self._join:
                self._reading_results_join()
            else:
                self._reading_results()
        return self._result


    def _reading_results(self):
        """
        reading results from file,
        doesn't check if everything is ready, i.e. if self.global_done"""
        for key_out in self._out_nm:
            self._result[key_out] = []
            if self.state_inputs:
                files = [name for name in glob.glob("{}/*/{}.txt".format(self.nodedir, key_out))]
                for file in files:
                    st_el = file.split(os.sep)[-2].split("_")
                    st_dict = collections.OrderedDict([(el.split(".")[0], eval(el.split(".")[1]))
                                                            for el in st_el])
                    with open(file) as fout:
                        logger.debug('Reading Results: file={}, st_dict={}'.format(file, st_dict))
                        self._result[key_out].append((st_dict, eval(fout.readline())))
            # for nodes without input
            else:
                files = [name for name in glob.glob("{}/{}.txt".format(self.nodedir, key_out))]
                with open(files[0]) as fout:
                    self._result[key_out].append(({}, eval(fout.readline())))


    # TODO: should be probably combine with _reading_results
    def _reading_results_join(self):
        """
        reading results from file,
        doesn't check if everything is ready, i.e. if self.global_done"""
        for key_out in self._out_nm:
            self._result[key_out] = []
            dir_red_l = [name for name in glob.glob("{}/*".format(self.nodedir))]
            for ii, dir_red in enumerate(dir_red_l):
                #to zmienic, wywalic _ i
                red_el = dir_red.split(os.sep)[-1].split("_")[1:]
                if red_el and red_el[0]:
                    red_dict = collections.OrderedDict([(el.split(".")[0], eval(el.split(".")[1]))
                                                       for el in red_el])
                else:
                    red_dict = {}
                self._result[key_out].append((red_dict, []))
                #pdb.set_trace()
                files = [name for name in glob.glob("{}/*/{}.txt".format(dir_red, key_out))]
                for file in files:
                    st_el = file.split(os.sep)[-2].split("_")
                    st_dict = collections.OrderedDict([(el.split(".")[0], eval(el.split(".")[1]))
                                                            for el in st_el])
                    with open(file) as fout:
                        self._result[key_out][ii][1].append((st_dict, eval(fout.readline())))
                    print("RESULT", key_out, self._result[key_out])


    # only if we ask for reduction interface
    @property
    def result_join_interf(self):
        if not self._join_interface:
            raise Exception("don't have join interface, can't provide result_join")
        else:
            if not self._result_join_interf:
                self._reading_results_join_interf()
        return self._result_join_interf


    def _reading_results_join_interf(self):
        """
        reading results of red_interface from file,
        #doesn't check if everything is ready, i.e. if self.global_done_join"""
        # TODO red_{}.txt powinno byc gdzies indziej, nie wiem dlaczego jest w glownym
        self._result_join_interf["red_{}".format(self._join_interface_input)] = []
        dir_red_l = [name for name in glob.glob("{}/*".format(self.nodedir))]
        logger.debug("_reading_results_join DIR RED L {}".format( dir_red_l))
        for ii, dir_red in enumerate(dir_red_l):
            red_el = dir_red.split(os.sep)[-1].split("_")
            try:
                red_dict = collections.OrderedDict([(el.split(".")[0], eval(el.split(".")[1]))
                                                   for el in red_el[1:]])
            except IndexError:
                red_dict = {}
            file_redu = os.path.join(dir_red,"red_{}.txt".format(self._join_interface_input))
            with open(file_redu) as fout:
                self._result_join_interf["red_{}".format(self._join_interface_input)].append((red_dict, eval(fout.readline())))


    @property
    def inputs(self):
        """Return the inputs of the underlying interface"""
        #return self._interface.inputs
        return self._inputs

    @inputs.setter
    def inputs(self, inputs):
        self._inputs = inputs
        self.state_inputs = self._inputs.copy()


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
        logger.debug("Run interface el, name={}, i={}, ind={}".format(self.name, i, ind))
        state_dict, inputs_dict = self._collecting_input_el(ind)
        logger.debug("Run interface el, name={}, inputs_dict={}, state_dict={}".format(
                                                            self.name, inputs_dict, state_dict))
        self._interface.run(inputs_dict)
        output = self._interface.output
        logger.debug("Run interface el, output={}".format(output))
        dir_nm_el = "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())])
        if self._joinByKey:
            dir_join = "join_" + "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items()) if i not in self._joinByKey])
        elif self._join:
            dir_join = "join_"
        if self._joinByKey or self._join:
            os.makedirs(os.path.join(self.nodedir, dir_join), exist_ok=True)
            dir_nm_el = os.path.join(dir_join, dir_nm_el)
        os.makedirs(os.path.join(self.nodedir, dir_nm_el), exist_ok=True)
        for key_out in list(output.keys()):
            with open(os.path.join(self.nodedir, dir_nm_el, key_out+".txt"), "w") as fout:
                fout.write(str(output[key_out]))


    def run_interface_join_el(self, state_dict, input_list):
        """ running interface one element for join_interface."""
        logger.debug("Run join interface el, name={}, state_dict={}".format(
            self.name, state_dict))
        self._join_interface.run({"mylist":input_list}) # should have a user specified name
        output = self._join_interface.output
        dir_nm_el = "join_" + "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())])
        key_red_interf = self._join_interface_input
        logger.debug("Run join interface el, FileName={}".format(os.path.join(self.nodedir, dir_nm_el, "red_" + key_red_interf + ".txt")))
        with open(os.path.join(self.nodedir, dir_nm_el, "red_" + key_red_interf + ".txt"), "w") as fout:
            fout.write(str(output["red_out"]))


    def _collecting_input_el(self, ind):
        state_dict = self.node_states.state_values(ind)
        inputs_dict = {k: state_dict[k] for k in self._inputs.keys()}
        # reading extra inputs that come from previous nodes
        for (from_node, from_socket, to_socket) in self.needed_outputs:
            dir_nm_el_from = "_".join(["{}.{}".format(i, j) for i, j in list(state_dict.items())
                                       if i in list(from_node.state_inputs.keys())])
            file_from = os.path.join(from_node.nodedir, dir_nm_el_from, from_socket+".txt")
            with open(file_from) as f:
                inputs_dict[to_socket] = eval(f.readline())
        return state_dict, inputs_dict

    def checking_input_el(self, ind):
        try:
            self._collecting_input_el(ind)
            return True
        except: #TODO specify
            return False


    def preparing_node(self):
        # adding directory (should have workflowdir already)
        self.nodedir = os.path.join(self.wfdir, self.fullname)
        os.makedirs(self.nodedir, exist_ok=True)
        self.node_states = state.State(state_inputs=self.state_inputs, mapper=self.state_mapper)


