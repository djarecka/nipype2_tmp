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


class MpWorker(object):
    def __init__(self, nr_proc=4): #should be none
        self.nr_proc = nr_proc
        self.pool = mp.Pool(processes=self.nr_proc)


    def run_el(self, interface, inp):
        print("W WORKER: RUN EL", inp)
        self.pool.apply_async(interface, (inp[0], inp[1]))


