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
from dflow_refiner.tutorial_VASP_op import VASP_Refiner
from dflow_refiner import Fixed_in_ref

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

vasp_in_para = Fixed_in_ref(
    cmd_list=['mpirun -np 16 /opt/software/vasp.5.4.4/bin/vasp_ncl'],
    out_list=['OUTCAR'],
    in_fmt='None',
    log_file='out.log',

    is_batch_exe=True,
    cpu_per_worker=16,
    num_worker=3,

    is_aux_public=True,
    rename2='POSCAR'
)

vasp_template = VASP_Refiner(in_para=vasp_in_para,
                             image="franklalalala/py_autorefiner",
                             executor=dispatcher_executor)

if __name__ == "__main__":
    wf = Workflow(name="vasp-refiner")
    init = upload_artifact(Path('./input_files/xyz'))
    public_aux = upload_artifact(Path('./input_files/public_files'))

    step_refine = Step(name='refine', template=vasp_template,
                       artifacts={'init': init, 'in_aux': public_aux})
    wf.add(step_refine)

    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="refine")[0]
    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/refine_output/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/refine_output/')
