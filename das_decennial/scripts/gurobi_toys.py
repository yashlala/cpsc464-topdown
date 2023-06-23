import sys
import os
import tempfile
import numpy as np
import gurobipy as gb

TOKENSERVER=os.environ['GRB_TOKENSERVER']
TOKENSERVER_PORT=os.environ['GRB_TOKENSERVER_PORT']

GUROBI_LIC_CREATE_FNAME = '/tmp/gurobi.lic'

if not os.path.exists(GUROBI_LIC_CREATE_FNAME):
    with tempfile.NamedTemporaryFile(suffix='.lic', mode='w') as tf:
        tf.write(f"TOKENSERVER={TOKENSERVER}\n")
        tf.write(f"PORT={TOKENSERVER_PORT}\n")
        tf.flush()
        os.rename(tf.name, GUROBI_LIC_CREATE_FNAME)
os.environ["GRB_LICENSE_FILE"] = GUROBI_LIC_CREATE_FNAME

env = gb.Env.OtherEnv('gurobi_toys.log','Census','DAS',0,'')
# model = gb.Model('model', env=env)
# vars = model.addVars(3, 2, vtype=gb.GRB.CONTINUOUS, lb=0, name='vars')

#try:
m = gb.Model("model", env=env)


# Create variables
x = m.addMVar(shape=4, vtype=gb.GRB.CONTINUOUS, name="x")
#x = m.addVars(2,2,vtype=gb.GRB.CONTINUOUS, name="x")
#x = m.addMVar((2,3),vtype=gb.GRB.CONTINUOUS, name="x")

# Set objective
b = np.array([1.0, 1.345, 2.0, 4.3])
# b = np.array([
#     [1, 2, 3],
#     [4, 5, 6]
# ])

m.setObjective(x @ x - 2 * b @ x, gb.GRB.MINIMIZE)
#m.setMObjective(np.identity(4), -2*b, np.sum(b ** 2), sense=gb.GRB.MINIMIZE)
#m.setMObjective(np.identity(4), -2*b, np.sum(b ** 2), xQ_L=x, xQ_R=x, xc=x, sense=gb.GRB.MINIMIZE)

# # Build (sparse) constraint matrix
# data = np.array([1.0, 2.0, 3.0, -1.0, -1.0])
# row = np.array([0, 0, 0, 1, 1])
# col = np.array([0, 1, 2, 0, 1])
#
# A = ss.csr_matrix((data, (row, col)), shape=(2, 3))
#
# # Build rhs vector
# rhs = np.array([4.0, -1.0])
#
# # Add constraints
# m.addConstr(A @ x <= rhs, name="c")

# Optimize model
m.optimize()

print(x.X)
print('Obj: %g' % m.objVal)

m.addConstr( x >= 1.5)

m.optimize()

print(x.X)
print('Obj: %g' % m.objVal)

# except gb.GurobiError as e:
#     print('Error code ' + str(e.errno) + ": " + str(e))
#
# except AttributeError:
#     print('Encountered an attribute error')
