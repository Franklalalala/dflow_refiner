import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from dflow import Step, Workflow, download_artifact, upload_artifact, Steps, InputArtifact, OutputArtifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages

from dflow_refiner.miniAutoSteper_op import buildOneStep

# ASE package is needed
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\ase')

# dflow package location!!!
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')


if __name__ == "__main__":
    prev_cages = upload_artifact(Path('./input_files/prev_cages'))
    info = upload_artifact(Path('./input_files/info.pickle'))
    raw = upload_artifact(Path('./input_files/3addon_raw'))
    pristine_cage = upload_artifact(Path('./input_files/C60_000001812opted.xyz'))

    batch_build = Step(name='batch-build',
                       template=PythonOPTemplate(op_class=buildOneStep,
                                                 image="franklalalala/py_autorefiner"),
                       artifacts={'prev_cooked': prev_cages,
                                  'prev_info': info,
                                  'pristine_cage': pristine_cage,
                                  'in_raw': raw
                                  },
                       parameters={'cutoff': {
                                       'mode': 'rank',
                                       'rank': 10
                                   }})

    wf = Workflow(name="one-build")
    wf.add(batch_build)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end_0 = wf.query_step(name='batch-build')[0]

    assert (step_end_0.phase == "Succeeded")

    download_artifact(artifact=step_end_0.outputs.artifacts['out_raw'], path=r'./output_batch/')



