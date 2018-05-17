import numpy as np
import itertools
from collections import namedtuple
import pdb

from . import auxiliary as aux

class State(object):
    def __init__(self, state_inputs, node_name, mapper=None, inp_ord_map = {}):
        self.state_inputs = state_inputs

        self._mapper = mapper
        self.node_name = node_name
        if self._mapper:
            # changing mapper (as in rpn), so I can read from left to right
            # e.g. if mapper=('d', ['e', 'r']), _mapper_rpn=['d', 'e', 'r', '*', '.']
            self._mapper_rpn = aux.mapper2rpn(self._mapper)
            self._input_names_mapper = [i for i in self._mapper_rpn if i not in ["*", "."]]
        else:
            self._mapper_rpn = []
            self._input_names_mapper = []
        # not all input field have to be use in the mapper, can be an extra scalar
        self._input_names = list(self.state_inputs.keys())
        # will use alphabetic order

        # dictionary[key=input names] = list of axes related to
        # e.g. {'r': [1], 'e': [0], 'd': [0, 1]}
        # ndim - int, number of dimension for the "final array" (that is not created)
        self._axis_for_input, self._ndim = aux.mapping_axis(self.state_inputs, self._mapper_rpn)

        # list of inputs variable for each axis
        # e.g. [['e', 'd'], ['r', 'd']]
        # shape - list, e.g. [2,3]
        self._input_for_axis, self._shape = aux.converting_axis2input(self.state_inputs,
                                                                    self._axis_for_input, self._ndim)

        # list of all possible indexes in each dim, will be use to iterate
        # e.g. [[0, 1], [0, 1, 2]]
        self._all_elements = [range(i) for i in self._shape]
        self._inp_ord_map = inp_ord_map


    def __getitem__(self, key):
        if type(key) is int:
            key = (key,)
        return self.state_values(key)

    # not used?
    #@property
    #def input_for_axis(self):
    #    return self._input_for_axis

    # not used?
    #@property
    #def axis_for_input(self):
    #    return self._axis_for_input


    @property
    def all_elements(self):
        return self._all_elements

    # not used?
    #@property
    #def mapper(self):
    #    return self._mapper


    @property
    def ndim(self):
        return self._ndim


    @property
    def shape(self):
        return self._shape


    def state_values(self, ind):

        from collections import OrderedDict
        if len(ind) > self._ndim:
            raise IndexError("too many indices")

        for ii, index in enumerate(ind):
            if index > self._shape[ii] - 1:
                raise IndexError("index {} is out of bounds for axis {} with size {}".format(index, ii, self._shape[ii]))

        state_dict = {}
        for input, ax in self._axis_for_input.items():
            # checking which axes are important for the input
            sl_ax = slice(ax[0], ax[-1]+1)
            # taking the indexes related to the axes
            ind_inp = ind[sl_ax]
            if input in self._inp_ord_map.keys():
                if len(ind_inp) == 1: #dj i think it has to be 1d
                    ind_inp = self._inp_ord_map[input][ind_inp[0]]
                else:
                    raise Exception("somtehing wrong with connecttions and mapper")
            state_dict[input] = self.state_inputs[input][ind_inp]

        # adding values from input that are not used in the mapper
        for input in set(self._input_names) - set(self._input_names_mapper):
            state_dict[input] = self.state_inputs[input]

        #print("In STATE_VALUES", state_dict.items())
        # returning a named tuple
        return OrderedDict(sorted(state_dict.items(), key=lambda t: t[0]))