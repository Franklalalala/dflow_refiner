import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.refiners import xTB_Refiner
from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from dflow_refiner.build import BuildWithSub
from dflow_refiner.calculators import simpleExe

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
        'cpu_per_node': 6,
        'gpu_per_node': 0,
        'group_size': 8,
        'queue_name': 'batch',
        'envs': {
            "OMP_STACKSIZE": "4G",
            "OMP_MAX_ACTIVE_LEVELS": "1",
            "DFLOW_WORKFLOW": "{{workflow.name}}",
            "DFLOW_POD": "{{pod.name}}"
        },
    }
)


if __name__ == "__main__":
    src_dir = upload_artifact(Path('./input_files/batch_raw_xyz'))

    a_build = Step(name='build',
                   template=PythonOPTemplate(BuildWithSub, image="franklalalala/py_autorefiner"),
                   artifacts={'in_workbase': src_dir},
                   )

    exe_core = {'cmd_list': ['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                'out_list': ['xtbopt.log', 'xtbopt.xyz']}

    an_exe = Step(name='exe',
                  template=PythonOPTemplate(simpleExe,
                                            image="franklalalala/py_autorefiner",
                                            slices=Slices(sub_path=True,
                                                          input_artifact=["workbase"]
                                                          ),
                                            ),
                  artifacts={'workbase': a_build.outputs.artifacts['out_workbase']},
                  parameters={'exe_core': exe_core},
                  executor=dispatcher_executor,
                  )

    wf = Workflow(name='wf004', parallelism=8)
    wf.add(a_build)
    wf.add(an_exe)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="exe")[0]

    assert (step_end.phase == "Succeeded")

    for i in range(100):
        step_end = wf.query_step(name="exe")[i]
        download_artifact(artifact=step_end.outputs.artifacts['outs'], path=r'./output_files/')
