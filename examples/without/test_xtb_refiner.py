import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow_refiner import xTB_Refiner, Fixed_in_ref
from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices, BigParameter

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

xtb_in_para = Fixed_in_ref(cmd_list=['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                           out_list= ['xtbopt.log', 'xtbopt.xyz'],
                           is_batch_exe=True,
                           num_worker=6,
                           cpu_per_worker=8,
                           base_node=0,
                           poll_interval=0.5,
                           )

xtb_template = xTB_Refiner(in_para=xtb_in_para,
                           image="franklalalala/py_autorefiner",
                           executor=dispatcher_executor,
                           name='xtb-templates'
                           )


if __name__ == "__main__":
    src_dir = upload_artifact(Path('./input_files/batch_raw_xyz'))
    info = upload_artifact(Path('./input_files/info.pickle'))

    xtb_ref = Step(name='test001',
                   template=xtb_template,
                   artifacts={'init': src_dir,
                              'info': info},
                   )


    wf = Workflow(name="wf003")
    wf.add(xtb_ref)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="test001")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')
