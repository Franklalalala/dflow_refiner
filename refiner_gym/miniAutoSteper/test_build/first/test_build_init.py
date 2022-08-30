import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow import Step, Workflow, download_artifact, upload_artifact, Steps, InputArtifact, OutputArtifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages

from dflow_refiner.miniAutoSteper_op import buildFirstStep

# ASE package is needed
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\ase')

# dflow package location!!!
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')


if __name__ == "__main__":
    pristine_cage = upload_artifact(Path('./C60_000001812opted.xyz'))
    # os.makedirs('init_build', exist_ok=True)
    workbase = upload_artifact(Path('1addon_raw'))

    simple_build = Step(name='init-build',
                   template=PythonOPTemplate(op_class=buildFirstStep,
                                             image="franklalalala/py_autorefiner"),
                   artifacts={'cage_path': pristine_cage,
                              'in_raw': workbase},
                   )

    wf = Workflow(name="first-build")
    wf.add(simple_build)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end_0 = wf.query_step(name='init-build')[0]

    assert (step_end_0.phase == "Succeeded")

    download_artifact(artifact=step_end_0.outputs.artifacts['out_raw'], path=r'./output_first/')



