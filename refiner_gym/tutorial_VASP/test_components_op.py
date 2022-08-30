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
from dflow_refiner.tutorial_VASP_op import vaspInGen, vaspParser
from dflow_refiner.build import BuildWithAux
from dflow_refiner.calculators import batchExe


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


if __name__ == "__main__":
    wf = Workflow(name="vasp-components")
    init = upload_artifact(Path('./input_files/xyz'))
    public_aux = upload_artifact(Path('./input_files/public_files'))


    step_inputGen = Step(name='inputGen',
                   template=PythonOPTemplate(vaspInGen, image="franklalalala/py_autorefiner"),
                   artifacts={'init': init})
    wf.add(step_inputGen)

    step_build = Step(name='build', template=PythonOPTemplate(BuildWithAux, image='franklalalala/py_autorefiner'),
                  artifacts={'in_workbase': step_inputGen.outputs.artifacts['out_raw'], 'in_aux': public_aux},
                  parameters={'aux_para': {
                      'is_aux_mixed': False,
                      'is_aux_public': True,
                      'rename2': 'POSCAR'
                  }})
    wf.add(step_build)

    step_exe = Step(name='exe', template=PythonOPTemplate(batchExe, image='franklalalala/py_autorefiner'),
               artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
               parameters={'core_para': {'cmd_list': ['mpirun -np 16 /opt/software/vasp.5.4.4/bin/vasp_ncl'],
                                         'out_list': ['OUTCAR'],
                                         'in_fmt': 'None',
                                         'log_file': 'out.log'},
                           'batch_para': {'base_node': 0, 'cpu_per_worker': 16,
                              'num_worker': 3, 'poll_interval': 0.5}},
                    executor=dispatcher_executor)
    wf.add(step_exe)

    step_parse = Step(
        name='parse',
        template=PythonOPTemplate(vaspParser, image='franklalalala/py_autorefiner'),
        artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
    )
    wf.add(step_parse)



    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="parse")[0]
    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/parse_output/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/parse_output/')


    a_step = wf.query_step(name="exe")[0]
    download_artifact(artifact=a_step.outputs.artifacts['out_cooking'], path=r'./output_files/exe_output/')

    a_step = wf.query_step(name="build")[0]
    download_artifact(artifact=a_step.outputs.artifacts['out_workbase'], path=r'./output_files/build_output/')

    a_step = wf.query_step(name="inputGen")[0]
    download_artifact(artifact=a_step.outputs.artifacts['out_raw'], path=r'./output_files/inputGen_output/')

