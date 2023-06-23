from fractions import Fraction as Fr

GEOLEVEL_BUDGET_PROP = [Fr(x, 871) for x in [51, 78, 51, 172, 519]]

subtr_lev_index = -1
add_lev_index = 1

# Reallocation part size (1024)
real_part_size = 871
# Level to substract PLB from proportion:
# (Block, 519)
g2sp = GEOLEVEL_BUDGET_PROP[subtr_lev_index] * real_part_size
# Level to add PLB to proportion:
# (Tract, 51)
g2ap = GEOLEVEL_BUDGET_PROP[add_lev_index] * real_part_size
# parts (1/1024) of geolevel PLB to substract
m = 19
# default allocation on level that's subracted from
qs = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))
# default allocation on level that's added to (happens to be the same here)
qa = (Fr(1,1024), Fr(1,512), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(1,1024), Fr(5,1024), Fr(5,1024), Fr(1,1024), Fr(17,1024), Fr(989,1024))

# subtracting all from the detailed query, it's index -1 (last)
qsubstracted = [qp * Fr(g2sp, g2sp - m) for qp in qs[:-1]] + [qs[-1] - sum(qp * Fr(m, g2sp - m) for qp in qs[:-1])]

# adding to the 'total' query, it's index 0 (first)
qadded = [qa[0] + sum(qp * Fr(m, g2ap + m) for qp in qa[1:])] + [qp * Fr(g2ap, g2ap + m) for qp in qa[1:]]

print(f"Q subtracted ({GEOLEVEL_BUDGET_PROP[subtr_lev_index]}):\n", '\n '.join([str(qp) for qp in qsubstracted]))
print(f"Q added ({GEOLEVEL_BUDGET_PROP[add_lev_index]}):\n", '\n '.join([str(qa) for qa in qadded]))
print("\n")
print(f"Q subtracted as props of total rho\n",  '\n '.join([str(qp * (g2sp - m)/ real_part_size) for qp in qsubstracted]))
print(f"Q added as props of total rho:\n",  '\n '.join([str(qa * (g2ap + m )/ real_part_size) for qa in qadded]))