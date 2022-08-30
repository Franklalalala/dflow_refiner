import os

from ase.build import bulk
from ase.io import write


os.makedirs('input_files/xyz', exist_ok=True)
os.chdir('input_files/xyz')

cu_fcc = bulk('Cu', 'fcc', a=3.6, cubic=True)
write(filename='fcc.xyz', images=cu_fcc)

cu_sc = bulk('Cu', 'sc', a=3.6)
write(filename='sc.xyz', images=cu_sc)

cu_bct = bulk('Cu', 'fcc', a=3.6, orthorhombic=True)
write(filename='bct.xyz', images=cu_bct)

