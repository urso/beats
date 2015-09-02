#!/usr/bin/env python

import random
import string

import names

import mc

NUM_COUNTERS = 10
NUM_KEYS = 100

UPDATES = 10000
PROB_COUNTER_OP = 0.2


def make_key_command(cmd):
    def go(key):
        length = max(4, int(random.gauss(8, 2)))
        value = ''.join([random.choice(string.ascii_letters)
                         for _ in xrange(length)])
        return cmd(key, value)
    return go


def run(mc):
    counters = [[names.get_first_name(), False] for _ in xrange(NUM_COUNTERS)]
    keys = [names.get_last_name() for _ in xrange(NUM_KEYS)]

    sample_set = make_key_command(mc.set)
    sample_add = make_key_command(mc.add)
    sample_replace = make_key_command(mc.replace)

    for _ in xrange(UPDATES):
        counter_op = random.random() < PROB_COUNTER_OP
        if counter_op:
            print("run counter command")
            counter = random.choice(counters)
            name, seen = counter
            if not seen:
                counter[1] = True
                mc.set(name, 0)
            else:
                op = random.choice([mc.incr, mc.decr])
                op(name)
        else:
            print("run key command")
            name = random.choice(keys)
            key_op = random.choice([
                mc.get,
                sample_set,
                sample_add,
                sample_replace,
                mc.delete])
            key_op(name)

if __name__ == '__main__':
    mc.run_tcp(run)
