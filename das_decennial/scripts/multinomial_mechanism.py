import sys, os
from typing import List, Tuple, Union

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from programs.engine.discrete_gaussian_utility import RationalScalarBernoulliExp

def multinomial_randomized_response(*, true_category, possible_categories: Union[List, Tuple], epsilon: Tuple[int, int] = (0, 1), rng):
    """
        This is an implimentation of the multinomial randomized response mechanism provided by Wang, Wu, and Hu in "Using
        Randomized Response for Differential Privacy Preserving Data Collection" (2016) without using floating point operations.
        Inputs:
                true_category: the true category of a categorical variable of a given record
                possible_categories: the universe of possible values said categorical variable can take
                epsilon: this function can be used in a mechanism that satisfies epsilon[0]/epsilon[1] local DP; see Wang, Wu, and Hu (2016) for more detail
                rng: pseudo-random number generator (see programs.engine.rngs)
        Output:
                res: an element of possible_categories
    """
    assert true_category in possible_categories, "true_category is not in possible_categories"
    true_index = [k for k, x in enumerate(possible_categories) if true_category == x][0]
    while True:
        x = rng.integers(low=0, high=len(possible_categories))
        if (x == true_index) or (RationalScalarBernoulliExp(gamma=epsilon, rng=rng) == 1):
            # Given some j in {0, ..., len(possible_categories)-1} such that j != true_index, at this point x satisfies
            # P(x == j) = 1/len(possible_categories) * exp(-epsilon) and also P(x == true_index) = 1/len(possible_categories).
            # Thus, P(x == j)/P(x == true_index) = exp(-epsilon). Since this also holds for each such j, and this function also
            # produces an output with probability one, this implies the output probabilities are the same as the ones provided
            # in (Wang, Wu and Hu; 2016).
            return possible_categories[x]
