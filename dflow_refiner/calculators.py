import os
import time
from pathlib import Path
from typing import List, Union, Optional
import subprocess
import shutil

from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate


class simpleExe(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'workbase': Artifact(Path),
            'exe_core': dict
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'outs': Artifact(type=List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        core_para = op_in['exe_core']
        cwd_ = os.getcwd()
        os.chdir(op_in['workbase'])
        cmd_list = core_para['cmd_list']
        outs = []

        if 'in_fmt' in core_para.keys() and core_para['in_fmt'] != None:
            for a_file in os.listdir('./'):
                if a_file.endswith(core_para['in_fmt']):
                    cmd_list.append(a_file)
                    break
        else:
            cmd_list.append(os.listdir('./')[0])

        if 'log_file' in core_para.keys() and core_para['log_file'] != None:
            log_file = core_para['log_file']
            cmd_list.append(f'>>{log_file}')

        os.system(' '.join(cmd_list))
        for an_output in core_para['out_list']:
            if os.path.exists(an_output):
                outs.append(Path(os.path.join(op_in['workbase'], an_output)))
        os.chdir(cwd_)
        op_out = OPIO({
            "outs": outs,
        })
        return op_out


class batchExe(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_cooking': Artifact(Path),
            'core_para': dict,
            'batch_para': dict
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooking': Artifact(type=List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        def _stack_a_job():
            os.chdir(job_list[tracker])
            if core_para['in_fmt']:
                for an_input in os.listdir('./'):
                    if an_input.endswith(core_para['in_fmt']):
                        cmd_list.append(an_input)
                        break
            else:
                cmd_list.append(os.listdir('./')[0])
            if core_para['log_file']:
                log_str = '>>' + core_para['log_file']
                cmd_list.append(log_str)

            cmd_line = ' '.join(cmd_list)
            start_node = batch_para['base_node'] + batch_para['cpu_per_worker'] * worker_id
            end_node = batch_para['base_node'] + batch_para['cpu_per_worker'] * (worker_id + 1) - 1

            res_list[worker_id] = subprocess.Popen(f'taskset -c {start_node}-{end_node} {cmd_line}', shell=True)

            del cmd_list[-1]
            if core_para['log_file']:
                del cmd_list[-1]
            os.chdir('./..')

        core_para = op_in['core_para']
        batch_para = op_in['batch_para']

        cmd_list = core_para['cmd_list']
        cwd_ = os.getcwd()
        os.chdir(op_in['in_cooking'])
        tracker = 0
        job_list = os.listdir('./')
        job_num = len(job_list)
        res_list = [None] * batch_para['num_worker']
        while tracker < job_num:
            worker_id = tracker
            if os.path.isdir(job_list[tracker]):
                _stack_a_job()
            tracker = tracker + 1
            if worker_id == batch_para['num_worker'] - 1:
                break

        while tracker < job_num:
            for worker_id, a_proc in enumerate(res_list):
                if tracker < job_num:
                    if a_proc.poll() == 0:
                        _stack_a_job()
                        tracker = tracker + 1
                else:
                    break
            if 'poll_interval' in batch_para.keys():
                time.sleep(batch_para['poll_interval'])

        for a_proc in res_list:
            if a_proc != None:
                a_proc.wait()
        outs = []
        cwd_2 = os.getcwd()
        for a_job in job_list:
            os.chdir(cwd_2)
            os.chdir(a_job)
            for an_output in os.listdir('./'):
                for a_fmt in core_para['out_list']:
                    if an_output.endswith(a_fmt):
                        outs.append(Path(os.path.join(op_in['in_cooking'], a_job, an_output)))
                        break
        os.chdir(cwd_)
        op_out = OPIO({
            "out_cooking": outs,
        })
        return op_out

class asePreSan(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'workbase': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooked': Artifact(Path),
            'info': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        # Change your calculator here!!
        from ase.calculators.emt import EMT
        from ase.io import read
        import pandas as pd

        cwd_ = os.getcwd()
        os.chdir(op_in['workbase'])
        name_list = []
        e_list = []
        for a_file in os.listdir('./'):
            file_name = os.path.splitext(a_file)[0]
            name_list.append(file_name)
            an_atoms = read(a_file)
            an_atoms.calc = EMT()
            e_list.append(an_atoms.get_potential_energy())
        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)
        info.to_pickle('info.pickle')
        os.rename(src=op_in['workbase'], dst='cooked')
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path('info.pickle')
        })
        return op_out







