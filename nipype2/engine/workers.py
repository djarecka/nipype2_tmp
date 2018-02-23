from __future__ import print_function, division, unicode_literals, absolute_import
from builtins import object
from collections import defaultdict

from future import standard_library
standard_library.install_aliases()

from copy import deepcopy
import re, os, pdb, time
import multiprocessing as mp
#import multiprocess as mp
import itertools

# one worker per node? NO
class MpWorker(object):
    def __init__(self, nr_proc=4): #should be none
        self.nr_proc = nr_proc
        self.pool = mp.Pool(processes=self.nr_proc)


    def run_el(self, interface, inp):
        #pdb.set_trace()
        #if inp[0] == 0:
        #    time.sleep(5)
        print("W WORKER: RUN EL", inp)
        self.pool.apply_async(interface, (inp[0], inp[1]))



#dj TODO: to pozostalosci tego co zabralam z klasy Node
# def run_interface_serial(self):
#     """ running run_interface_el for each element
#         if plugin=serial.
#     """
#     results_list = []
#
#     # this should yield at the end, not append to the list
#     # pdb.set_trace()
#     for i, ind in enumerate(itertools.product(*self.node_states._all_elements)):
#         state_dict, output = self.run_interface_el(ind)
#         results_list.append((i, state_dict, output))
#
#     # the shortest version, without any reducer
#     if not self._reducer:
#         return results_list
#
#     # have reducer
#     elif self._reducer:
#         # pdb.set_trace()
#         results_reduce_dict = defaultdict(list)
#         for (i, st, out) in results_list:
#             if self._reducer == "all":
#                 results_reduce_dict["all"].append((i, st, out))
#             else:
#                 red_key = st[self._reducer]
#                 results_reduce_dict[red_key].append((i, st, out))
#         # no reduction fun
#         if not self._reducer_interface:
#             return results_reduce_dict.items()
#         # reducer and reducing function
#         else:
#             results_red_list = []
#             for i, red_l in results_reduce_dict.items():
#                 out_l = [el[2]["out"] for el in red_l]
#                 inp_dict = {}
#                 inp_dict["out"] = out_l
#                 out_red = self.run_red_interface_el(inp_dict)
#                 results_red_list.append((i, out_red))
#             return results_red_list
#
#
# def run_interface_el_mp(self, input_mp, output_mp):
#     """ running run_interface_el and saving in mp.Queue """  # TODO
#     # pdb.set_trace()
#     for i, ind in iter(input_mp.get, 'STOP'):
#         print("IN RUN INTER, os.getpid = ", os.getpid())
#         state_dict, output = self.run_interface_el(ind)
#         output_mp.put((i, state_dict, output))
#
#
# def run_red_interface_el_mp(self, input_mp, output_mp):
#     """ running run_red_interface_el and saving in mp.Queue """  # TODO
#     # TODO" it should be probably combined with run_interface_el_mp
#     for i, red_l in iter(input_mp.get, 'STOP'):
#         out_l = [el[2]["out"] for el in red_l]
#         print("IN RUN RED INTER, os.getpid = ", out_l)  # os.getpid())
#         inp_dict = {}
#         inp_dict["out"] = out_l
#         out_red = self.run_red_interface_el(inp_dict)
#         output_mp.put((i, out_red))
#
#
# def run_interface_mp(self):
#     """ running run_interface_el_mp for each element
#          if plugin=mp (using multiprocessing).
#     """
#     task_queue = mp.Queue()
#     done_queue = mp.Queue()
#
#     for (i, ind) in enumerate(itertools.product(*self.node_states._all_elements)):
#         task_queue.put((i, ind))
#     task_nr = i + 1
#     # pdb.set_trace()
#     # Start worker processes
#     NUMBER_OF_PROCESSES = 4  # TODO what should be this number??
#     for i in range(NUMBER_OF_PROCESSES):
#         mp.Process(target=self.run_interface_el_mp, args=(task_queue, done_queue)).start()
#     # do I have to join those processes??
#
#     # Tell child processes to stop
#     for i in range(NUMBER_OF_PROCESSES):
#         task_queue.put('STOP')
#     # pdb.set_trace()
#     # the shortest version, without any reducer
#     if not self._reducer:
#         results_list = [done_queue.get() for p in range(task_nr)]
#         return results_list
#
#     # have reducer
#     elif self._reducer:
#         # pdb.set_trace()
#         results_reduce_dict = defaultdict(list)
#         for _ in range(task_nr):
#             (i, st, out) = done_queue.get()
#             if self._reducer == "all":
#                 results_reduce_dict["all"].append((i, st, out))
#             else:
#                 red_key = st[self._reducer]
#                 results_reduce_dict[red_key].append((i, st, out))
#         # no reduction fun
#         if not self._reducer_interface:
#             return results_reduce_dict.items()
#         # reducer and reducing function
#         else:
#             task_red_queue = mp.Queue()
#             done_red_queue = mp.Queue()
#
#             for i, el in enumerate(results_reduce_dict.items()):
#                 task_red_queue.put(el)
#             task_red_nr = i + 1
#
#             for i in range(NUMBER_OF_PROCESSES):
#                 mp.Process(target=self.run_red_interface_el_mp, args=(task_red_queue, done_red_queue)).start()
#
#             # Tell child processes to stop
#             # dj: I shouldn't have more? you never know if 2 STOPS don't go to the same child..
#             for i in range(NUMBER_OF_PROCESSES):
#                 task_red_queue.put('STOP')
#
#             results_red_list = [done_red_queue.get() for p in range(task_red_nr)]
#             print("results_red_list", results_red_list)
#             return results_red_list

