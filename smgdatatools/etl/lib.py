import math

import cftime
import numpy as np

from smgdatatools.collector.lib import Collector


class Etl:
    def run(self, dest: str, collector: Collector, stores: list, aggregations: list[str]):
        raise NotImplementedError


def to_numpy(arr):
    return np.array(arr)


def convert_times(times, frm_units, frm_calendar, to_units, to_calendar):
    dates = cftime.num2date(times, frm_units, frm_calendar)
    return cftime.date2num(dates, to_units, to_calendar)


def join_existing(variables):
    vs = sorted(variables, key=lambda x: x.store.name, reverse=False)
    i = 0
    for v in vs:
        for chunk in v.chunks:
            chunk.index = i
            i += 1

    return vs


def calculate_chunk_idx(variable, index):
    # order dimensions by it's index in the dataspace
    dimensions = sorted(variable.dimensions, key=lambda x: x.index, reverse=False)

    # chunk_count by dimension was stored in collection phase
    nchunks = [d.chunk_count for d in dimensions]

    # if a single chunk_shape was stored, need to expand to the number of chunks in that dimension and
    # get the bounds (see https://zarr.dev/zeps/draft/ZEP0003.html)
    # chunk_shapes = [x.chunk_shapes if len(x.chunk_shapes) == nchunks[i] else x.chunk_shapes * nchunks[i]
    #                 for i, x in enumerate(dimensions)]
    # chunk_shapes_bounds = [cumsum([y.shape for y in x]) for x in chunk_shapes]

    idx = list()
    temp = index
    for i, nchunk in enumerate(nchunks):
        f = nchunks[i + 1:]
        mult = 1
        for x in f:
            mult *= x
        bound = math.floor(temp / mult)
        idx.append(bound)
        temp = temp - (mult * bound)

    return idx
