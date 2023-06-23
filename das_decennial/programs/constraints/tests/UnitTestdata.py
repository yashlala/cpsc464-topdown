import numpy as np
from collections import Counter
units = [
            # each row is a unit
            # columns: 'hhgq','unit UID',
            [0, 0],
            [0, 1],
            [0, 2],
            [0, 3],
            [1, 4],
            [15, 5],
            [16, 6],
            [20, 7],
            [29, 8],
       ]
counts = Counter(np.array(units)[:,0])
gq_counts = tuple(zip(*((k-1,v) for k,v in counts.items() if k > 1)))
hu_count = sum([v for k,v in counts.items() if k < 2])
all_counts = ((0,)+gq_counts[0], (hu_count,)+gq_counts[1])