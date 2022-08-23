import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow_refiner import ABC_Refiner, Fixed_in_ref
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

local_minima_folder = 'job1'
abc_in_para = Fixed_in_ref(cmd_list=['/home/mkliu/polymer/ABCluster/geom'],
                           out_list=['info.out', local_minima_folder],
                           log_file='info.out',
                           in_fmt='inp',

                           is_batch_exe=True,
                           num_worker=3,
                           cpu_per_worker=16,
                           base_node=0,
                           poll_interval=0.5,

                           is_aux_mixed=False,
                           is_aux_public=False,

                           ctm_para={'LM_folder_name': local_minima_folder}
                           )

abc_template = ABC_Refiner(in_para=abc_in_para,
                           image="franklalalala/py_autorefiner",
                           executor=dispatcher_executor,
                           name='ref-abc'
                           )


if __name__ == "__main__":
    src_dir = upload_artifact(Path('./input_files/hand_tuned_structures'))
    public_inp = upload_artifact(Path('./input_files/c6h12.inp'))


    abc_ref = Step(name='abc',
                   template=abc_template,
                   artifacts={'init': src_dir, 'public_inp': public_inp},
                   )


    wf = Workflow(name='test')
    wf.add(abc_ref)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="abc")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')
