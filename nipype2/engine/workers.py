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

from .. import config, logging
logger = logging.getLogger('workflow')


class MpWorker(object):
    def __init__(self, nr_proc=4): #should be none
        self.nr_proc = nr_proc
        self.pool = mp.Pool(processes=self.nr_proc)
        logger.debug('Initialize Worker')


    def run_el(self, interface, inp):
        self.pool.apply_async(interface, (inp[0], inp[1]))


    def close_pool(self):
        # added this method since I was having somtetimes problem with reading results from (existing) files
        # i thought that pool.close() should work, but still was getting some errors, so testing terminate
        self.pool.terminate()


