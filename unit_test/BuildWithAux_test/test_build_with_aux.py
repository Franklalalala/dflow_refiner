import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union


from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from dflow_refiner.build import BuildWithAux

# dflow package location!!!
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')

if __name__ == "__main__":
    input_root = os.path.abspath('./real_INPUT_files')
    input_art = upload_artifact(Path(input_root))

    # Single private auxiliary
    # ============================================================================
    os.makedirs('temp_AUX_sgl_pvt', exist_ok=True)
    os.chdir('temp_AUX_sgl_pvt')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name+'.txt')
    os.chdir('./..')
    sgl_pvt_art = upload_artifact(Path('temp_AUX_sgl_pvt'))
    sgl_pvt_step = Step(name='sgl-pvt', template=PythonOPTemplate(BuildWithAux, image='franklalalala/py_autorefiner'),
                  artifacts={'in_workbase': input_art, 'in_aux': sgl_pvt_art},
                  parameters={'aux_para': {
                      'is_aux_mixed': False,
                      'is_aux_public': False,
                  }})
    wf = Workflow(name='test-sgl-pvt')
    wf.add(sgl_pvt_step)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="sgl-pvt")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_workbase'], path=r'./real_OUTPUT_files/single_private')
    # ===============================================================================



    # Single public auxiliary
    # ============================================================================
    os.makedirs('temp_AUX_sgl_pbc', exist_ok=True)
    os.chdir('temp_AUX_sgl_pbc')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name)
    os.chdir('./..')
    sgl_pbc_art = upload_artifact(Path('temp_AUX_sgl_pbc'))
    sgl_pbc_step = Step(name='sgl-pbc', template=PythonOPTemplate(BuildWithAux, image='franklalalala/py_autorefiner'),
                  artifacts={'in_workbase': input_art, 'in_aux': sgl_pbc_art},
                  parameters={'aux_para': {
                      'is_aux_mixed': False,
                      'is_aux_public': True,
                      'rename2': 'POSCAR'
                  }})
    wf = Workflow(name='test-sgl-pbc')
    wf.add(sgl_pbc_step)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="sgl-pbc")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_workbase'], path=r'./real_OUTPUT_files/single_public')
    # ===============================================================================


    # Multiple private auxiliary
    # ============================================================================
    os.makedirs('temp_AUX_multi_pvt', exist_ok=True)
    os.chdir('temp_AUX_multi_pvt')

    os.makedirs('pvt1', exist_ok=True)
    os.chdir('pvt1')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name+'.txt')
    os.chdir('./..')

    os.makedirs('pvt2', exist_ok=True)
    os.chdir('pvt2')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name+'.out')
    os.chdir('./../..')


    multi_pvt_art = upload_artifact(Path('temp_AUX_multi_pvt'))
    multi_pvt_step = Step(name='multi-pvt', template=PythonOPTemplate(BuildWithAux, image='franklalalala/py_autorefiner'),
                  artifacts={'in_workbase': input_art, 'in_aux': multi_pvt_art},
                  parameters={'aux_para': {
                      'is_aux_mixed': False,
                      'pvt_aux_list': ['pvt1', 'pvt2']
                  }})
    wf = Workflow(name='test-multi-pvt')
    wf.add(multi_pvt_step)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="multi-pvt")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_workbase'], path=r'./real_OUTPUT_files/multiple_private')
    # ===============================================================================



    # Mixed scenario
    # ============================================================================
    os.makedirs('temp_AUX_mixed', exist_ok=True)
    os.chdir('temp_AUX_mixed')

    os.makedirs('pvt1', exist_ok=True)
    os.chdir('pvt1')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name+'.txt')
    os.chdir('./..')

    os.makedirs('pvt2', exist_ok=True)
    os.chdir('pvt2')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name+'.out')
    os.chdir('./..')

    os.makedirs('pbc', exist_ok=True)
    os.chdir('pbc')
    for an_input in os.listdir(input_root):
        a_name = os.path.splitext(an_input)[0]
        shutil.copy(src=os.path.join(input_root, an_input), dst=a_name)
    os.chdir('./../..')


    mixed_art = upload_artifact(Path('temp_AUX_mixed'))
    mixed_step = Step(name='mixed', template=PythonOPTemplate(BuildWithAux, image='franklalalala/py_autorefiner'),
                  artifacts={'in_workbase': input_art, 'in_aux': mixed_art},
                  parameters={'aux_para': {
                      'is_aux_mixed': True,
                      'pvt_aux_list': ['pvt1', 'pvt2'],
                      'pbc_aux_name': 'pbc'
                  }})
    wf = Workflow(name='test-mixed')
    wf.add(mixed_step)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="mixed")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_workbase'], path=r'./real_OUTPUT_files/Mixed')
    # ===============================================================================




