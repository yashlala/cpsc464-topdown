"""
Topdown engine with no noise infusion
"""

# python imports
import numpy as np

# das-created imports
from programs.engine.topdown_engine import TopdownEngine
import programs.engine.primitives as primitives


def unprotected_answer(*, true_data, query, inverse_scale: float, mechanism_name, bounded_dp=True):
    """Answer not protected via the differential privacy routines in this code base"""
    return TopdownNoiselessEngine.NoNoiseMechanism(inverse_scale, 1.0, query.answer(true_data))

class TopdownNoiselessEngine(TopdownEngine):
    """
    Topdown engine with no noise infusion
    """

    class NoNoiseMechanism(primitives.DPMechanism):
        """Mechanism that doesn't infuse noise. Keeping it here, with the only engine we'll use it"""

        def __init__(self, epsilon: float, sensitivity: float, true_answer: np.ndarray):
            self.epsilon = np.inf
            self.variance = 1.e-7  # Can't be zero, because the inverse is used as a child weight
            self.protected_answer = true_answer
            self.sensitivity = sensitivity

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.mechanism = unprotected_answer
