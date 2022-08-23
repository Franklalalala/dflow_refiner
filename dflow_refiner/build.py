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


class simpleBuild(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_workbase': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_workbase': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = 'cooking'
        cwd_ = os.getcwd()
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        if os.path.isdir(os.path.join(op_in['in_workbase'], 'raw')):
            raw = os.path.abspath(os.path.join(op_in['in_workbase'], 'raw'))
        else:
            raw = os.path.abspath(op_in['in_workbase'])
        for a_file in os.listdir(raw):
            os.chdir(dst)
            a_file_name = os.path.splitext(a_file)[0]
            os.makedirs(a_file_name, exist_ok=True)
            shutil.copy(src=os.path.join(raw, a_file), dst=os.path.join(a_file_name, a_file))
        os.chdir(cwd_)
        op_out = OPIO({
            "out_workbase": Path(name),
        })
        return op_out


class BuildWithSub(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_workbase': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_workbase': Artifact(type=List[Path], archive=None),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = 'cooking'
        cwd_ = os.getcwd()
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        if os.path.isdir(os.path.join(op_in['in_workbase'], 'raw')):
            raw = os.path.abspath(os.path.join(op_in['in_workbase'], 'raw'))
        else:
            raw = os.path.abspath(op_in['in_workbase'])
        outs = []
        for a_file in os.listdir(raw):
            os.chdir(dst)
            a_file_name = os.path.splitext(a_file)[0]
            os.makedirs(a_file_name, exist_ok=True)
            shutil.copy(src=os.path.join(raw, a_file), dst=os.path.join(a_file_name, a_file))
            outs.append(Path(os.path.join(name, a_file_name)))
        os.chdir(cwd_)
        op_out = OPIO({
            "out_workbase": outs,
        })
        return op_out




# Build with auxiliary files.
# if is_private is true, files in in_aux folder should contain identical names with in_workbase folder
# if false, it should be comman files(repeated in every workbase)
class BuildWithAux(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_workbase': Artifact(Path),
            'in_aux': Artifact(Path),
            'aux_para': dict
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_workbase': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        def _private_unit():
            an_aux_fmt = os.path.splitext(os.listdir(private_folder)[0])[1]
            for an_input in os.listdir(input_root):
                os.chdir(dst)
                a_file_name = os.path.splitext(an_input)[0]
                os.makedirs(a_file_name, exist_ok=True)
                os.chdir(a_file_name)
                shutil.copy(src=os.path.join(private_folder, a_file_name + an_aux_fmt), dst=a_file_name + an_aux_fmt)
                shutil.copy(src=os.path.join(input_root, an_input), dst=an_input)
        def _public_unit():
            for an_input in os.listdir(input_root):
                os.chdir(dst)
                a_file_name = os.path.splitext(an_input)[0]
                os.makedirs(a_file_name, exist_ok=True)
                os.chdir(a_file_name)
                shutil.copy(src=os.path.join(input_root, an_input), dst=an_input)
                for a_public_file in os.listdir(public_folder):
                    shutil.copy(src=os.path.join(public_folder, a_public_file), dst=a_public_file)

        name = 'cooking'
        cwd_ = os.getcwd()
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)

        input_root = os.path.abspath(op_in['in_workbase'])
        aux_root = os.path.abspath(op_in['in_aux'])
        aux_para = op_in['aux_para']
        if 'is_aux_mixed' in aux_para.keys() and aux_para['is_aux_mixed'] == True:
            public_folder = os.path.join(aux_root, aux_para['pbc_aux_name'])
            _public_unit()
            for a_private in aux_para['pvt_aux_list']:
                private_folder = os.path.join(aux_root, a_private)
                _private_unit()
        elif 'pvt_aux_list' in aux_para.keys() and aux_para['pvt_aux_list'] != None:
            for a_private in aux_para['pvt_aux_list']:
                private_folder = os.path.join(aux_root, a_private)
                _private_unit()
        elif 'is_aux_public' in aux_para.keys() and aux_para['is_aux_public'] == True:
            public_folder = aux_root
            _public_unit()
        else:
            private_folder = aux_root
            _private_unit()

        os.chdir(cwd_)
        op_out = OPIO({
            "out_workbase": Path(name),
        })
        return op_out


class Empty(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_s': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_s': str
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        op_out = OPIO({
            "out_s": op_in['in_s'],
        })
        return op_out



