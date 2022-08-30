import os
import time
from pathlib import Path
from typing import List, Optional, Union, Dict
import subprocess
import shutil

from dflow import Step, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate
import pandas as pd


# ===============================================================================================================
# Prepare customized inputGen OP and parser OP
# ===============================================================================================================
class vaspInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
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
        from ase.io.vasp import write_vasp

        cwd_ = os.getcwd()
        os.makedirs('raw', exist_ok=True)
        dst = os.path.abspath('raw')
        os.chdir(op_in['init'])

        for a_file in os.listdir('./'):
            file_name = os.path.splitext(a_file)[0]
            atoms = read(a_file)
            write_vasp(file=os.path.join(dst, file_name), atoms=atoms)

        os.chdir(cwd_)
        op_out = OPIO({
            'out_raw': Path('raw'),
        })
        return op_out


class vaspParser(OP):
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
        from ase.io.vasp import read_vasp_out
        from ase.io import write

        cwd_ = os.getcwd()
        name = 'cooked'
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        name_list = []
        e_list = []
        os.chdir(os.path.join(op_in['in_cooked'], 'cooking'))
        for a_job in os.listdir('./'):
            old_path = os.path.join(a_job, 'OUTCAR')
            if os.path.exists(old_path):
                name_list.append(a_job)
                out_atoms = read_vasp_out(old_path)
                e = out_atoms.get_potential_energy() / len(out_atoms)
                e_list.append(e)
                new_xyz_path = os.path.join(dst, a_job + '.xyz')
                write(new_xyz_path, images=out_atoms)
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

# ===============================================================================================================
# Build Refiner OP
# ===============================================================================================================
from dflow_refiner.refiners import Refiner, Fixed_in_ref
from dflow_refiner.build import BuildWithAux
from dflow_refiner.calculators import batchExe


class VASP_Refiner(Refiner):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor,
                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(VASP_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs,
                                          outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.inputs.artifacts['in_aux'] = InputArtifact()
        self.prefix = self.name
        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(vaspInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init']},
            key=f"{self.prefix}-inputGen",
        )
        self.add(step_inputGen)

        step_build = Step(
            name="build",
            template=PythonOPTemplate(BuildWithAux, image=image),
            artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw'],
                       'in_aux': self.inputs.artifacts['in_aux']},
            parameters={'aux_para': in_para.aux},
            key=f"{self.prefix}-build",
        )

        self.add(step_build)

        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'core_para': in_para.exe_core, 'batch_para': in_para.exe_batch},
            executor=executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        step_parse = Step(
            name='parse',
            template=PythonOPTemplate(vaspParser, image=image),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']



