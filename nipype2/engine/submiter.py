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
        self.done = queue.Queue()
        if self.plugin == "mp":
            self.worker = MpWorker(done=self.done)
        logger.debug('Initialize Submitter, graph: {}'.format(graph))



    def run_workflow(self):
        for (i_n, node) in enumerate(self.graph):
            # checking if a node has all input (doesnt have to wait for others)
            if node.sufficient:
                self.submit_work(node)
            # if its not, its been added to a line
            else:
                self.node_line.append(node)

        # TODO: should add an extra condition to not stay here forever
        while self.node_line:
            logger.debug("Submitter, node_line BEFORE trying to get new output: {}".format(self.node_line))
            try:
                el_done = self.done.get(timeout=1)
                logger.debug("Submitter, el from self.done: {}".format(el_done))
                time.sleep(2)
                self.connecting_output(el_done)
            except queue.Empty:
                time.sleep(3)
            logger.debug("Submitter, node_line AFTER trying to get new output: {}".format(self.node_line))


        # final reading of the results, this will be removed in the final version (TODO)
        # combining all results from specifics nodes together (for all state elements)
        time.sleep(3)
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
                # for nodes without input
                else:
                    files = [name for name in glob.glob("{}/{}.txt".format(node.nodedir, key_out))]
                    with open(files[0]) as fout:
                        node._result[key_out].append(({}, eval(fout.readline())))


    def connecting_output(self, el_out):
        from_node = el_out[2]
        for (from_socket, to_node, to_socket) in from_node.sending_output:
            if (from_node, from_socket, to_socket) in to_node.needed_outputs:
                # TODO this works only because there is no mapper!
                #pdb.set_trace()
                print("FILE OUTPUT", [name for name in glob.glob("{}/*/{}.txt".format(from_node.nodedir, from_socket))])
                file_output = [name for name in glob.glob("{}/*/{}.txt".format(from_node.nodedir, from_socket))][0]
                with open(file_output) as f:
                    to_node.inputs.update({to_socket: eval(f.readline())})
                to_node.needed_outputs.remove((from_node, from_socket, to_socket))

                if not to_node.needed_outputs:
                    print("TO NODE", to_node)
                    pdb.set_trace()
                    self.node_line.remove(to_node)
                    to_node.sufficient = True
                    self.submit_work(to_node)
            else:
                raise Exception("something wrong with connections")


    def submit_work(self, node):
        node.node_states_inputs = State(state_inputs=node._inputs, mapper=node._mapper,
                                        inp_ord_map=node._input_order_map)
        for (i, ind) in enumerate(itertools.product(*node.node_states._all_elements)):
            logger.debug("SUBMIT WORKER, node: {}, ind: {}".format(node, ind))
            self.worker.run_el(node.run_interface_el, (i, ind), node)





# not beeing used now (probably will be removed)
    def _waiting_for_output(self):
        # checking which node is ready to go. should be done my callback (TODO)
        while self.node_line:
            logger.debug("Submitter, node_line: {}".format(self.node_line))
            for i, node in enumerate(self.node_line):
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
                    self.node_line.remove(node)
                    node.sufficient = True
                    self.submit_work(node)
            time.sleep(3)
