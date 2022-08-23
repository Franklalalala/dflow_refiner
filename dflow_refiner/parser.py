import os
import time
from pathlib import Path
from typing import List, Optional, Union
import subprocess
import shutil

from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate
import pandas as pd


def get_gau_e_list(gau_log_path: Union[str, Path]):
    e_list = []
    with open(gau_log_path, 'r') as f:
        for a_line in f.readlines():
            if a_line.startswith(' SCF Done'):
                a_e = float(a_line.split()[4])
                e_list.append(a_e)
    return e_list


class xtbParser(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_cooked': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooked': Artifact(Path),
            'info': Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd_ = os.getcwd()
        name = 'cooked'
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        name_list = []
        e_list = []
        os.chdir(os.path.join(op_in['in_cooked'], 'cooking'))
        for a_job in os.listdir('./'):
            old_path = os.path.join(a_job, 'xtbopt.xyz')
            if os.path.exists(old_path):
                name_list.append(a_job)
                with open(old_path, 'r') as f:
                    f.readline()
                    e_line = f.readline()
                e_list.append(float(e_line.split()[1]))
                new_path = os.path.join(dst, a_job + '.xyz')
                shutil.copy(src=old_path, dst=new_path)
        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)
        info.to_pickle('info.pickle')
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path('info.pickle')
        })
        return op_out


class gaussParser(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_cooked': Artifact(Path),
            'prefix': str,
            'keep_chk': bool
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooked': Artifact(Path),
            'info': Artifact(Path),
            'cooked_chk': Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from dflow_refiner.ase_gaussian_io import read_gaussian_out
        from ase.units import Hartree, eV
        from ase.io import write

        cwd_ = os.getcwd()
        prefix = op_in['prefix']
        if op_in['keep_chk']:
            chk_folder_name = 'cooked_chk'
            chk_path = os.path.abspath(chk_folder_name)
            os.makedirs(chk_path, exist_ok=True)

        cooked_name = 'cooked'
        dst = os.path.abspath(cooked_name)
        os.makedirs(dst, exist_ok=True)
        name_list = []
        e_list = []
        os.chdir(os.path.join(op_in['in_cooked'], 'cooking'))
        for a_job in os.listdir('./'):
            old_path = os.path.join(a_job, f'{a_job}.log')
            if os.path.exists(old_path):
                name_list.append(f'{prefix}_{a_job}')
                with open(old_path, 'r') as f:
                    gau_atoms = read_gaussian_out(fd=f)
                a_e = get_gau_e_list(gau_log_path=old_path)[-1]
                e_list.append(a_e)
                new_path = os.path.join(dst, f'{prefix}_{a_job}.xyz')
                write(filename=new_path, images=gau_atoms, format='xyz')
            if op_in['keep_chk']:
                old_path = os.path.join(a_job, f'{a_job}.chk')
                if os.path.exists(old_path):
                    shutil.copy(src=old_path, dst=os.path.join(chk_path, f'{prefix}_{a_job}.chk'))

        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)

        info.to_pickle(f'{prefix}_info.pickle')
        if op_in['keep_chk']:
            chk_out = Path('cooked_chk')
        else:
            with open('null', 'w') as f:
                pass
            chk_out = Path('null')
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path(f'{prefix}_info.pickle'),
            'cooked_chk': chk_out
        })
        return op_out


class abcParser(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_cooked': Artifact(Path),
            'LM_folder': str,
            'prefix': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooked': Artifact(Path),
            'info': Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        def _parse_unit():
            with open('info.out', 'r') as f:
                f_lines = f.readlines()
                for idx, a_line in enumerate(f_lines):
                    if a_line.startswith('All minima are saved to '):
                        break
                idx = idx + 4
                for a_line in f_lines[idx:]:
                    if a_line.startswith('------'):
                        break
                    else:
                        new_name, old_name, e, _ = a_line.split()
                        new_name = f'{prefix}_{job_name}_LM_{new_name}'
                        new_path = os.path.join(dst, new_name + '.xyz')
                        shutil.copy(src=os.path.join(op_in['LM_folder'], old_name + '.xyz'), dst=new_path)
                        e_list.append(float(e))
                        name_list.append(new_name)

        prefix = op_in['prefix']
        cwd_ = os.getcwd()
        name = 'cooked'
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        name_list = []
        e_list = []
        if 'cooking' in os.listdir(op_in['in_cooked']):
            os.chdir(os.path.join(op_in['in_cooked'], 'cooking'))
            for job_name in os.listdir('./').copy():
                os.chdir(job_name)
                _parse_unit()
                os.chdir('./..')
        else:
            os.chdir(op_in['in_cooked'])
            os.chdir(os.listdir('./')[0])
            job_name = 'init0'
            _parse_unit()
        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)
        info.to_pickle(f'{prefix}_info.pickle')
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path(f'{prefix}_info.pickle')
        })
        return op_out



















