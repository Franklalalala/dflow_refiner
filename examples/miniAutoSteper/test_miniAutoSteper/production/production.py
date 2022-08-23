# ==================================================================================================================
# This is a production version of 'test_miniautosteper', it simply enlarged the cutoff criteria.
# =====================================================================================================================

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow import Step, Workflow, download_artifact, upload_artifact, Steps, InputArtifact, OutputArtifact, Inputs, \
    Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages
from dflow_refiner import Gau_Refiner, xTB_Refiner, Fixed_in_ref
from dflow_refiner.miniAutoSteper_op import *
from dflow_refiner.refiners import Refiner
from dflow_refiner.tools import unfix_steps_in_para

# ASE package is needed
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\ase')

# dflow package location!!!
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')

dispatcher_executor = DispatcherExecutor(
    image="franklalalala/py_autorefiner",
    machine_dict={
        'remote_root': r'/home/mkliu/test_dpdispatcher/',
        'batch_type': 'Torque',
        'remote_profile': {
            "hostname": "219.245.39.76",
            "username": "mkliu",
            "password": "mkliu123",
            'port': 22
        }
    },
    resources_dict={
        'number_node': 1,
        'cpu_per_node': 48,
        'gpu_per_node': 0,
        'group_size': 5,
        'queue_name': 'batch',
        'envs': {
            "DFLOW_WORKFLOW": "{{workflow.name}}",
            "DFLOW_POD": "{{pod.name}}"
        },
    }
)

# grow unit
# =========================================================================================================
xtb_in_para = Fixed_in_ref(cmd_list=['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                           out_list=['xtbopt.log', 'xtbopt.xyz'],
                           is_batch_exe=True,
                           num_worker=8,
                           cpu_per_worker=6,
                           cutoff_mode='rank',
                           cutoff_rank=1000
                           )
xtb_tmp = xTB_Refiner(name=f"tmp-xtb",
                      in_para=xtb_in_para,
                      image="franklalalala/py_autorefiner",
                      executor=dispatcher_executor)
xtb_tmp = unfix_steps_in_para(old_steps=xtb_tmp, unfix_para='core_para')
grow_in_para = Fixed_in_ref(cutoff_mode='rank',
                            cutoff_rank=200,
                            ctm_para={
                                'old_core_para': xtb_in_para.exe_core,
                                'prescan_min_e_gap': 5E-3,
                                'ase_command': r'/home/mkliu/anaconda3/envs/molnet/bin/python3.8',
                                'xtb_template': xtb_tmp
                            },
                            cmd_list=[],
                            out_list=[])

init_grow_unit = grow_unit(in_para=grow_in_para,
                           image="franklalalala/py_autorefiner",
                           executor=dispatcher_executor,
                           name='init-grow-unit',
                           is_first=True
                           )

a_grow_unit = grow_unit(in_para=grow_in_para,
                        image="franklalalala/py_autorefiner",
                        executor=dispatcher_executor,
                        name='a-grow-unit',
                        is_first=False
                        )
# =========================================================================================================

# gau template
# ====================================================================================================
gau_opt_in = Fixed_in_ref(cmd_list=['g16'],
                          out_list=['log', 'chk'],
                          is_batch_exe=True,
                          num_worker=2,
                          cpu_per_worker=24,
                          cutoff_mode='rank',
                          cutoff_rank=20,
                          ctm_para={'keep_chk': True}
                          )
gau_opt_template = Gau_Refiner(in_para=gau_opt_in,
                               image="franklalalala/py_autorefiner",
                               executor=dispatcher_executor,
                               name='opt-gau'
                               )
gau_sp_in = Fixed_in_ref(cmd_list=['g16'],
                         out_list=['log', 'chk'],
                         in_fmt='gjf',
                         is_batch_exe=True,
                         num_worker=1,
                         cpu_per_worker=48,
                         is_aux_public=False,
                         is_aux_mixed=False,
                         cutoff_mode='rank',
                         cutoff_rank=5,
                         ctm_para={'has_chk_input': True, 'keep_chk': True}
                         )
gau_sp_template = Gau_Refiner(in_para=gau_sp_in,
                              image="franklalalala/py_autorefiner",
                              executor=dispatcher_executor,
                              name='sp-gau'
                              )
# =======================================================================================================


if __name__ == "__main__":
    wf = Workflow(name="test-sim")
    final_addon_num = 10
    pristine_cage = upload_artifact(Path('./C60_000001812opted.xyz'))

    init_one_step = Step(name="init-step", template=init_grow_unit,
                         artifacts={'init': pristine_cage},
                         parameters={'addon_num': 1})
    wf.add(init_one_step)

    # grow simulation
    # =========================================================================================================
    grow_sim = Steps(name="a-loop",
                     inputs=Inputs(
                         parameters={"addon_num": InputParameter(),
                                     "limit": InputParameter(value=final_addon_num + 1)},
                         artifacts={
                             'prev_cooked': InputArtifact(),
                             'prev_info': InputArtifact(),
                             'pristine_cage': InputArtifact()
                         }))
    one_step = Step(name="one-step", template=a_grow_unit,
                    parameters={"addon_num": grow_sim.inputs.parameters["addon_num"]},
                    artifacts={'prev_cooked': grow_sim.inputs.artifacts['prev_cooked'],
                               'info': grow_sim.inputs.artifacts['prev_info'],
                               'init': pristine_cage
                               })
    grow_sim.add(one_step)

    grow_next = Step(name="grow-next", template=grow_sim,
                     parameters={"addon_num": one_step.outputs.parameters["addon_num"]},
                     artifacts={'prev_cooked': one_step.outputs.artifacts['out_cooked'],
                                'prev_info': one_step.outputs.artifacts['info'],
                                'pristine_cage': pristine_cage},
                     when="%s < %s" % (
                         one_step.outputs.parameters["addon_num"],
                         grow_sim.inputs.parameters["limit"]))
    grow_sim.add(grow_next)

    grow_sim_loop = Step(name='loop', template=grow_sim,
                         parameters={"addon_num": init_one_step.outputs.parameters['addon_num'],
                                     "limit": final_addon_num + 1},
                         artifacts={
                             'prev_cooked': init_one_step.outputs.artifacts['out_cooked'],
                             'prev_info': init_one_step.outputs.artifacts['info'],
                             'pristine_cage': pristine_cage
                         })
    wf.add(grow_sim_loop)
    # ==========================================================================================================

    gau_opt = Step(name='gau-opt',
                   template=gau_opt_template,
                   artifacts={'init': OutputArtifact(global_name='global_xtb_out_cooked'),
                              'info': OutputArtifact(global_name='global_xtb_info')},
                   parameters={'cmd_line': r' opt b3lyp/3-21G',
                               'charge': 0,
                               'multi': 1,
                               'prefix': 'opted'}
                   )
    wf.add(gau_opt)

    gau_sp = Step(name='gau-sp',
                  template=gau_sp_template,
                  artifacts={'init': gau_opt.outputs.artifacts['out_cooked'],
                             'info': gau_opt.outputs.artifacts['info'],
                             'in_chk': gau_opt.outputs.artifacts['cooked_chk']},
                  parameters={'cmd_line': r'b3lyp/6-311G(d, p) guess=TCheck',
                              'charge': 0,
                              'multi': 1,
                              'prefix': '6-311G-d-p',
                              },
                  )
    wf.add(gau_sp)

    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")

    step_init = wf.query_step(name="init-step")[0]
    download_artifact(artifact=step_init.outputs.artifacts['out_cooked'], path=r'./output_files/1addons/')
    download_artifact(artifact=step_init.outputs.artifacts['info'], path=r'./output_files/1addons/')

    for i in range(2, final_addon_num + 1):
        one_step = wf.query_step(name="one-step")[i - 2]
        download_artifact(artifact=one_step.outputs.artifacts['out_cooked'], path=f'./output_files/{i}addons/')
        download_artifact(artifact=one_step.outputs.artifacts['info'], path=f'./output_files/{i}addons/')

    gau_opt = wf.query_step(name="gau-opt")[0]
    download_artifact(artifact=gau_opt.outputs.artifacts['out_cooked'], path=f'./output_files/gau_opt')
    download_artifact(artifact=gau_opt.outputs.artifacts['info'], path=f'./output_files/gau_opt')

    gau_sp = wf.query_step(name="gau-sp")[0]
    download_artifact(artifact=gau_sp.outputs.artifacts['out_cooked'], path=f'./output_files/gau_sp')
    download_artifact(artifact=gau_sp.outputs.artifacts['info'], path=f'./output_files/gau_sp')
    download_artifact(artifact=gau_sp.outputs.artifacts['cooked_chk'], path=f'./output_files/gau_sp')
