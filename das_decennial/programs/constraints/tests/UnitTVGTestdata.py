import numpy as np
from collections import Counter
units = [
            # each row is a unit
            # columns: 'tenvacgq','unit UID',
            [0, 0],   # Mortgage
            [1, 1],   # Owned
            [2, 2],   # Rented
            [3, 3],   # No Pay
            [4, 4],   # Vacant
            [15, 5],  # 'GQ 105 Correctional residential facilities'
            [16, 6],  # 'GQ 106 Military disciplinary barracks'
            [20, 7],  # 'GQ 301 Nursing facilities': [20]
            [29, 8],  # 'GQ 701 Emergency and transitional shelters'
       ]

counts = Counter(np.array(units)[:, 0])
gq_counts = tuple(zip(*((k - 4, v) for k, v in counts.items() if k > 4)))
hu_count = sum([v for k, v in counts.items() if k < 5])
all_counts = ((0,) + gq_counts[0], (hu_count,) + gq_counts[1])
