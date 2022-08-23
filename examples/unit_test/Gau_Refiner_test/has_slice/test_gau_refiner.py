import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from dflow_refiner import Gau_Refiner, Fixed_in_ref
from dflow_refiner.tools import steps_slc_in_para

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
gau_in_para = Fixed_in_ref(cmd_list=['g16'],
                           out_list= ['log', 'chk'],

                           is_batch_exe=True,
                           num_worker=4,
                           cpu_per_worker=12,
                           base_node=0,
                           poll_interval=0.5,

                           cutoff_mode='rank',
                           cutoff_rank=4
                           )

gau_template = Gau_Refiner(in_para=gau_in_para,
                           image="franklalalala/py_autorefiner",
                           executor=dispatcher_executor,
                           name='gau-opt'
                           )

if __name__ == "__main__":
    src_dir = upload_artifact(Path('./input_files/batch_raw_xyz'))
    info = upload_artifact(Path('./input_files/info.pickle'))

    gau_template = steps_slc_in_para(old_steps=gau_template, slc_para=['cmd_line', 'prefix'])

    gau_ref = Step(name='gau-ref',
                   template=gau_template,
                   artifacts={'init': src_dir, 'info': info},
                   parameters={'cmd_line': [r'b3lyp/6-311G empiricaldispersion=GD3',r'b3lyp/6-311G(d, p) empiricaldispersion=GD3'],
                               'charge': 0,
                               'multi': 1,
                               'prefix': ['6-311G', '6-311G-d-p'],
                               'slice_id': '{{item}}'},
                   with_param=argo_range(2)
                   )

    wf = Workflow(name="test-slice-gau")
    wf.add(gau_ref)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="gau-ref")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')

    step_end = wf.query_step(name="gau-ref")[1]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')


