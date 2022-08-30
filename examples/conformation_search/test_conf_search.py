import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from dflow_refiner import ABC_Refiner, xTB_Refiner, Gau_Refiner, Fixed_in_ref
from dflow_refiner.tools import steps_slc_in_para

# dflow_refiner package location!!!
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')
# ASE package is needed
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\ase')

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

local_minima_folder = 'job1'  # This parameter is pre-defined in ABCluster files.
abc_in_para = Fixed_in_ref(cmd_list=['/home/mkliu/polymer/ABCluster/geom'],
                           out_list=['info.out', local_minima_folder],
                           log_file='info.out',
                           in_fmt='inp',
                           is_batch_exe=True,
                           num_worker=4,
                           cpu_per_worker=12,
                           is_aux_mixed=False,
                           is_aux_public=False,
                           ctm_para={'LM_folder_name': local_minima_folder}
                           )

abc_gen_template = ABC_Refiner(in_para=abc_in_para,
                               image="franklalalala/py_autorefiner",
                               executor=dispatcher_executor,
                               name='abc-gen'
                               )

xtb_in_para = Fixed_in_ref(cmd_list=['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                           out_list=['xtbopt.log', 'xtbopt.xyz'],
                           is_batch_exe=True,
                           num_worker=6,
                           cpu_per_worker=8,
                           )

xtb_opt_template = xTB_Refiner(in_para=xtb_in_para,
                               image="franklalalala/py_autorefiner",
                               executor=dispatcher_executor,
                               name='xtb-opt'
                               )

gau_opt_in = Fixed_in_ref(cmd_list=['g16'],
                          out_list=['log', 'chk'],
                          is_batch_exe=True,
                          num_worker=4,
                          cpu_per_worker=12,
                          cutoff_mode='rank',
                          cutoff_rank=4,
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
                         num_worker=2,
                         cpu_per_worker=12,
                         is_aux_public=False,
                         is_aux_mixed=False,
                         cutoff_mode='rank',
                         cutoff_rank=2,
                         ctm_para={'has_chk_input': True, 'keep_chk': True}
                         )

gau_sp_template = Gau_Refiner(in_para=gau_sp_in,
                              image="franklalalala/py_autorefiner",
                              executor=dispatcher_executor,
                              name='sp-gau'
                              )

if __name__ == "__main__":
    wf = Workflow(name='conf-search')

    src_dir = upload_artifact(Path('./input_files/hand_tuned_structures'))
    public_inp = upload_artifact(Path('./input_files/c6h12.inp'))

    abc_gen = Step(name='abc',
                   template=abc_gen_template,
                   artifacts={'init': src_dir, 'public_inp': public_inp},
                   )
    wf.add(abc_gen)

    xtb_opt = Step(name='xtb',
                   template=xtb_opt_template,
                   artifacts={'init': abc_gen.outputs.artifacts['out_cooked'],
                              'info': abc_gen.outputs.artifacts['info']},
                   )
    wf.add(xtb_opt)

    gau_opt = Step(name='gau-opt',
                   template=gau_opt_template,
                   artifacts={'init': xtb_opt.outputs.artifacts['out_cooked'],
                              'info': xtb_opt.outputs.artifacts['info']},
                   parameters={'cmd_line': r' opt b3lyp/6-31G',
                               'charge': 0,
                               'multi': 1,
                               'prefix': 'opted'}
                   )
    wf.add(gau_opt)

    gau_sp_template = steps_slc_in_para(old_steps=gau_sp_template, slc_para=['cmd_line', 'prefix'])
    gau_sp = Step(name='gau-sp',
                  template=gau_sp_template,
                  artifacts={'init': gau_opt.outputs.artifacts['out_cooked'],
                             'info': gau_opt.outputs.artifacts['info'],
                             'in_chk': gau_opt.outputs.artifacts['cooked_chk']},
                  parameters={'cmd_line': [r'b3lyp/def2tzvpp guess=TCheck', r'b3lyp/6-311G(d, p) guess=TCheck'],
                              'charge': 0,
                              'multi': 1,
                              'prefix': ['def2tzvpp', '6-311G-d-p'],
                              'slice_id': '{{item}}'},
                  with_param=argo_range(2)
                  )

    wf.add(gau_sp)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    for i in range(2):
        step_end = wf.query_step(name="gau-sp")[i]

        assert (step_end.phase == "Succeeded")

        download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
        download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')
        download_artifact(artifact=step_end.outputs.artifacts['cooked_chk'], path=r'./output_files/')
