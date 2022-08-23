import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union, Optional, Dict

from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range, Steps, Inputs, InputArtifact, \
    InputParameter, OutputArtifact, Outputs, OutputParameter
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
import pandas as pd
import numpy as np
from dflow_refiner.miniAutoSteper_func import *
from dflow_refiner.inputGen import get_low_e_isomer
from dflow_refiner.refiners import Refiner
from dflow_refiner import xTB_Refiner, Fixed_in_ref
from dflow_refiner.tools import unfix_steps_in_para


# ===============================================================================================================
# For miniAutoSteper!!!
# ===============================================================================================================

class customizedASEPreSan(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'workbase': Artifact(Path),
            'min_e_gap': float
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
        from torchlightmolnet.caculator import torchCaculator
        from torchlightmolnet.lightning.molnet import LightMolNet
        from torchlightmolnet.dataset.atomref import refatoms_xTB, get_refatoms
        from torchlightmolnet import Properties
        import torch
        from ase.io import read
        from ase.units import Hartree
        import pandas as pd

        cwd_ = os.getcwd()
        os.chdir(op_in['workbase'])

        net = LightMolNet(atomref=get_refatoms(refatoms_xTB)[Properties.energy_U0])
        state_dict = torch.load(r'/home/mkliu/schnet_opt/paper_4_27/Cl_final.ckpt')
        net.load_state_dict(state_dict["state_dict"])
        calculator = torchCaculator(net=net)
        name_list = []
        e_list = []
        for a_file in os.listdir('./'):
            file_name = os.path.splitext(a_file)[0]
            name_list.append(file_name)
            an_atoms = read(a_file)
            an_atoms.calc = calculator
            e_list.append(an_atoms.get_total_energy()[0][0])

        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)
        info = de_redundancy(old_df=info, interval=op_in['min_e_gap'] / Hartree, val_clm_name='e')
        info.to_pickle('info.pickle')
        os.makedirs(name='cooked')
        for a_name in info['name']:
            shutil.copy(src=os.path.join(op_in['workbase'], a_name + '.xyz'),
                        dst=os.path.join('cooked', a_name + '.xyz'))
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path('info.pickle')
        })
        return op_out


class EvenOddSwitch(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_core_para': dict,
            'addon_num': int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_core_para': dict,
            'out_addon_num': int,
            'out_build_workbase': Artifact(Path)
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        addon_num = op_in['addon_num']
        in_core_para = op_in['in_core_para']
        if addon_num % 2 == 1:
            in_core_para['cmd_list'].append('--uhf 1')
        os.makedirs(f'{addon_num}addon_raw')
        op_out = OPIO({
            'out_build_workbase': Path(f'{addon_num}addon_raw'),
            'out_addon_num': addon_num + 1,
            'out_core_para': in_core_para
        })
        return op_out


class buildFirstStep(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'cage_path': Artifact(Path),
            'in_raw': Artifact(Path),
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
        import numpy as np
        from numpy import pi
        from ase.atoms import Atom, Atoms
        from ase.io import read, write
        from ase.neighborlist import build_neighbor_list

        cwd_ = os.getcwd()
        old_cage = read(op_in['cage_path'], format='xyz')
        old_coords = old_cage.get_positions()
        cage_centre = old_cage.get_positions().mean(axis=0)
        neighborList = build_neighbor_list(old_cage, bothways=True, self_interaction=False)
        dok_matrix = neighborList.get_connectivity_matrix()
        cage_size = len(old_cage)
        max_add_36_size = len(to_36_base(int('1' * cage_size, 2)))

        os.chdir(op_in['in_raw'])
        for i in range(cage_size):
            name, addon_set, _ = seq2name(seq=f'{i}', cage_size=cage_size, cage_max_add_36_size=max_add_36_size)
            build_unit(old_cage=old_cage, old_coords=old_coords, addon_set=[i], dok_matrix=dok_matrix,
                       cage_centre=cage_centre, name=name)

        os.chdir(cwd_)
        op_out = OPIO({
            'out_raw': Path(op_in['in_raw']),
        })
        return op_out


class buildOneStep(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'prev_cooked': Artifact(Path),
            'prev_info': Artifact(Path),
            'cutoff': dict,
            'pristine_cage': Artifact(Path),
            'in_raw': Artifact(Path),
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
        import numpy as np
        from numpy import pi
        from ase.atoms import Atom, Atoms
        from ase.io import read, write
        from ase.neighborlist import build_neighbor_list

        cwd_ = os.getcwd()
        pristine_cage = read(op_in['pristine_cage'])
        cage_centre = pristine_cage.get_positions().mean(axis=0)
        cage_size = len(pristine_cage)
        max_add_36_size = len(to_36_base(int('1' * cage_size, 2)))
        abs_prev_path = os.path.abspath(op_in['prev_cooked'])
        os.chdir(op_in['in_raw'])
        new_name_set = set()
        prev_info = pd.read_pickle(op_in['prev_info'])
        for a_name in get_low_e_isomer(info=prev_info, para=op_in['cutoff']):
            old_cage = read(filename=os.path.join(abs_prev_path, a_name + '.xyz'), format='xyz')
            old_coords = old_cage.get_positions()
            neighborList = build_neighbor_list(old_cage, bothways=True, self_interaction=False)
            dok_matrix = neighborList.get_connectivity_matrix()
            old_seq, old_addon_set, _0 = name2seq(name=a_name, cage_size=cage_size)
            for ii in range(cage_size):
                if ii not in old_addon_set:
                    new_seq = old_seq + f' {str(ii)}'
                    name, _, _0 = seq2name(seq=new_seq, cage_max_add_36_size=max_add_36_size, cage_size=cage_size)
                    if name in new_name_set:
                        continue
                    new_name_set.add(name)
                    build_unit(old_cage=old_cage, old_coords=old_coords, addon_set=[ii], dok_matrix=dok_matrix,
                               cage_centre=cage_centre, name=name)
        os.chdir(cwd_)
        op_out = OPIO({
            'out_raw': Path(op_in['in_raw']),
        })
        return op_out


class grow_unit(Refiner):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor, is_first: bool = True,
                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(grow_unit, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs,
                                        outputs=outputs,
                                        memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.inputs.parameters['addon_num'] = InputParameter()
        self.prefix = self.inputs.parameters['addon_num']
        even_odd_switch = Step(name=f"switch-{self.prefix}",
                               template=PythonOPTemplate(EvenOddSwitch, image=image,
                                                         output_parameter_global_name={'out_core_para':
                                                                                           'global_xtb_core_para'}),
                               parameters={'addon_num': self.prefix,
                                           'in_core_para': in_para.ctm['old_core_para']},
                               key=f"switch-{self.prefix}")
        self.add(even_odd_switch)
        if is_first:
            step_build = Step(name=f"build-{self.prefix}",
                              template=PythonOPTemplate(buildFirstStep, image=image),
                              artifacts={'cage_path': self.inputs.artifacts['init'],
                                         'in_raw': even_odd_switch.outputs.artifacts['out_build_workbase']},
                              key=f"build-{self.prefix}", )
        else:
            self.inputs.artifacts['prev_cooked'] = InputArtifact()
            step_build = Step(name=f"build-{self.prefix}",
                              template=PythonOPTemplate(buildOneStep, image=image),
                              artifacts={'prev_cooked': self.inputs.artifacts['prev_cooked'],
                                         'prev_info': self.inputs.artifacts['info'],
                                         'pristine_cage': self.inputs.artifacts['init'],
                                         'in_raw': even_odd_switch.outputs.artifacts['out_build_workbase']
                                         },
                              parameters={'cutoff': in_para.cutoff},
                              key=f"build-{self.prefix}"
                              )
        self.add(step_build)
        ase_prescan = Step(name=f"prescan-{self.prefix}",
                           template=PythonOPTemplate(op_class=customizedASEPreSan,
                                                     image=image,
                                                     command=in_para.ctm['ase_command']),
                           artifacts={'workbase': step_build.outputs.artifacts['out_raw']},
                           parameters={'min_e_gap': in_para.ctm['prescan_min_e_gap']},
                           executor=executor,
                           key=f"prescan-{self.prefix}"
                           )
        self.add(ase_prescan)

        # Direct transfer of 'core_para' from step_build to xtb_relax will cause undesired yaml file.
        xtb_relax = Step(name=f"xtb-relax-{self.prefix}",
                         template=in_para.ctm['xtb_template'],
                         artifacts={'init': ase_prescan.outputs.artifacts['out_cooked'],
                                    'info': ase_prescan.outputs.artifacts['info']},
                         parameters={'core_para': '{{workflow.outputs.parameters.global_xtb_core_para}}'},
                         key=f"xtb-relax-{self.prefix}",
                         )

        self.add(xtb_relax)

        self.outputs.artifacts['info']._from = xtb_relax.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = xtb_relax.outputs.artifacts['out_cooked']

        self.outputs.parameters['addon_num'] = OutputParameter()
        self.outputs.parameters['addon_num'].value_from_parameter = even_odd_switch.outputs.parameters['out_addon_num']
