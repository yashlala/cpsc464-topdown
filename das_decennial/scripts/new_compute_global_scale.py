from math import sqrt; from fractions import Fraction as F; import numpy

def compute_geoproportions_global_scale(prev_global_scale_str, prev_geo_proportions, new_rho_US):
    prev_global_scale = F(prev_global_scale_str)
    prev_rho = 1/prev_global_scale**2
    prev_g_US = F(prev_geo_proportions[0])
    prev_g_ST = F(prev_geo_proportions[1])
    prev_g_CTY = F(prev_geo_proportions[2])
    prev_g_TR = F(prev_geo_proportions[3])
    prev_g_BG = F(prev_geo_proportions[4])
    prev_g_B = F(prev_geo_proportions[5])
    rho_CTY = F(prev_g_CTY * prev_rho)
    rho_TR = F(prev_g_TR * prev_rho)
    rho_BG = F(prev_g_BG * prev_rho)
    rho_B = F(prev_g_B * prev_rho)
    print(f'rho_CTY: {rho_CTY}; float(rho_CTY): {float(rho_CTY)}')
    print(f'rho_BG: {rho_BG}; float(rho_BG): {float(rho_BG)}')
    print(f'rho_B: {rho_B}; float(rho_B): {float(rho_B)}')

    rho_US = rho_ST = new_rho_US
    print(f'rho_US: {rho_US}; float(rho_US): {float(rho_US)}')

    rho = rho_US + rho_ST + rho_CTY + rho_TR + rho_BG + rho_B
    print(f'total rho: {rho}; float(rho): {float(rho)}')

    global_scale = sqrt(1/rho)
    g_US = g_ST = rho_US/rho
    g_CTY = rho_CTY/rho
    g_TR = rho_TR/rho
    g_BG = rho_BG/rho
    g_B = rho_B/rho
    print(f'global_scale: {global_scale}')
    print(f'g_US: {g_US}; float(g_US): {float(g_US)}')
    print(f'g_CTY: {g_CTY}; float(g_CTY): {float(g_CTY)}')
    print(f'g_TR: {g_TR}; float(g_TR): {float(g_TR)}')
    print(f'g_BG: {g_BG}; float(g_BG): {float(g_BG)}')
    print(f'g_B: {g_B}; float(g_B): {float(g_B)}')

    float_list = [float(g_US), float(g_ST), float(g_CTY), float(g_TR), float(g_BG), float(g_B)]
    geo_props = [F(int(numpy.ceil(float_f * 1024)), 1024) for float_f in float_list]
    print(f'Global scale value is: {F(int(numpy.floor(float(global_scale) * 1024)), 1024)}')
    print(f'geo_proportions are: {geo_props}, with sum of: {sum(geo_props)}')


if __name__ == "__main__":
    prev_geo_proportions = ['495/1024','495/1024','1/512','1/512','3/512','3/128']
    prev_global_scale_str = '225/1024'
    new_rho_US = 1

    compute_geoproportions_global_scale(prev_global_scale_str, prev_geo_proportions, new_rho_US)


    '''

    With the global_scale and geopropotions from Row 3 in the Tracker (opt_spine, 1aTotIso):

    ```
    prev_geo_proportions = ['495/1024','495/1024','1/512','1/512','3/512','3/128']
    prev_global_scale_str = '225/1024'
    new_rho_US = 1
    ```

    And called with:
    ```
    python scripts/new_compute_global_scale.py
    ```

    Gives:
    rho_CTY: 2048/50625; float(rho_CTY): 0.04045432098765432
    rho_BG: 2048/16875; float(rho_BG): 0.12136296296296296
    rho_B: 8192/16875; float(rho_B): 0.48545185185185186
    rho_US: 1; float(rho_US): 1.0
    total rho: 136066/50625; float(rho): 2.6877234567901236
    global_scale: 0.609968923402129
    g_US: 50625/136066; float(g_US): 0.37206208751635234
    g_CTY: 1024/68033; float(g_CTY): 0.015051519115723251
    g_TR: 1024/68033; float(g_TR): 0.015051519115723251
    g_BG: 3072/68033; float(g_BG): 0.045154557347169756
    g_B: 12288/68033; float(g_B): 0.18061822938867902
    Global scale value is: 39/64
    geo_proportions are: [Fraction(381, 1024), Fraction(381, 1024), Fraction(1, 64), Fraction(1, 64), Fraction(47, 1024), Fraction(185, 1024)], with sum of: 513/512

    So the results are:
    ```
    US = 381/1024
    State (ST) = 381/1024
    County (CTY) = 1/64 == 16/1024
    Tract (TR) = 1/64 == 16/1024
    Block_Group (BG) = 47/1024
    Block (B) = 185/1024
    ```

    The total is 1026/1024. we need it to be equal to 1 (1024/1024). So we subtract 1 each from US and State (the largest proportions)

    US = 381/1024 --> 380/1024
    State (ST) = 381/1024 --> 380/1024

    Giving us, for `opt_spine`, `1aTotIso`, parameters of:
    Giving a new geo_proportion of: 380/1024 380/1024 16/1024 16/1024 47/1024 185/1024
    And a global_scale of: 39/64

    '''
