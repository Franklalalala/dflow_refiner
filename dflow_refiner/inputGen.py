import os
import shutil
from pathlib import Path
from typing import Optional, List
import numpy as np
import pandas as pd
from dflow.python import OP, OPIO, Artifact, OPIOSign



def get_low_e_isomer(info: pd.DataFrame, para: dict):
    name_list = info['name']
    if para['mode'] == 'None':
        for a_name in name_list:
            yield a_name
    elif para['mode'] == 'rank':
        for a_name in name_list[:para['rank']]:
            yield a_name
    else:
        from ase.units import Hartree, eV
        e_arr = np.array(info['e']) * Hartree / eV
        e_arr = e_arr - min(e_arr)
        uncutted_e = []
        for a_e in e_arr:
            if a_e > para['value']:
                break
            uncutted_e.append(a_e)
        if para['mode'] == 'value':
            for idx in range(len(uncutted_e)):
                yield name_list[idx]
        else:
            if para['mode'] == 'value_and_rank' or para['mode'] == 'rank_and_value':
                for idx in range(min(len(uncutted_e), para['rank'])):
                    yield name_list[idx]
            if para['mode'] == 'value_or_rank' or para['mode'] == 'rank_or_value':
                for idx in range(max(len(uncutted_e), para['rank'])):
                    yield name_list[idx]


class simpleGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_xyz': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_gjf': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from ase.io import read
        from ase.io.gaussian import write_gaussian_in
        atoms = read(op_in['in_xyz'])
        with open('gauss_in.gjf', 'w') as f:
            write_gaussian_in(fd=f,
                              atoms=atoms,
                              properties=[' '],
                              method="",
                              basis=" opt b3lyp/6-311G(d, p) empiricaldispersion=GD3",
                              chk='gauss_in.chk',
                              nprocshared='48',
                              mem='10GB',
                              )
        op_out = OPIO({
            "out_gjf": Path('gauss_in.gjf'),
        })
        return op_out


class gaussInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'info': Artifact(Path),
            'cutoff': dict,
            'cmd_line': str,
            'charge': int,
            'multi': int,
            'cpu_per_worker': int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from ase.io import read
        from ase.io.gaussian import write_gaussian_in

        cwd_ = os.getcwd()
        os.makedirs('raw', exist_ok=True)
        info = pd.read_pickle(op_in['info'])
        abs_in_dir = os.path.abspath(op_in['init'])
        os.chdir('raw')
        for a_name in get_low_e_isomer(info=info, para=op_in['cutoff']):
            dst_gjf = a_name+'.gjf'
            dst_chk = a_name+'.chk'
            in_atoms = read(filename=os.path.join(abs_in_dir, a_name+'.xyz'))
            with open(dst_gjf, 'w') as f:
                write_gaussian_in(fd=f,
                                  atoms=in_atoms,
                                  properties=[' '],
                                  method='',
                                  basis=op_in['cmd_line'],
                                  nprocshared=str(op_in['cpu_per_worker']),
                                  mem='10GB', # 10GB by default
                                  mult=op_in['multi'],
                                  charge=op_in['charge'],
                                  chk=dst_chk
                                  )
        os.chdir(cwd_)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out


class xtbInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'info': Artifact(Path),
            'cutoff': dict,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd_ = os.getcwd()
        os.makedirs('raw', exist_ok=True)
        info = pd.read_pickle(op_in['info'])
        abs_in_dir = os.path.abspath(op_in['init'])
        os.chdir('raw')
        for a_name in get_low_e_isomer(info=info, para=op_in['cutoff']):
            shutil.copy(src=os.path.join(abs_in_dir, a_name+'.xyz'), dst=a_name+'.xyz')
        os.chdir(cwd_)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out

class abcInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'public_inp': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:

        public_inp_path = op_in['public_inp']
        os.makedirs('raw', exist_ok=True)
        for old_name in os.listdir(op_in['init']):
            new_name = os.path.splitext(old_name)[0] + '.inp'
            new_path = os.path.join('raw', new_name)
            with open(public_inp_path, 'r') as f_r, open(file=new_path, mode='w') as f_w:
                f_r_lines = f_r.readlines()
                for idx, a_line in enumerate(f_r_lines):
                    f_w.writelines(a_line)
                    if a_line.startswith('components'):
                        break
                key_line = f_r_lines[idx + 1]
                new_key_line = old_name + ' ' + key_line.split()[1] + '\n'
                f_w.writelines(new_key_line)
                for a_line in f_r_lines[idx + 2:]:
                    f_w.writelines(a_line)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out











