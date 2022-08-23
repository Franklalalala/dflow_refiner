import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union, Optional

from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, Slices
import pandas as pd
import numpy as np


def steps_slc_in_para(old_steps: Steps, slc_para: Union[List[str], str]):
    if isinstance(slc_para, str):
        slc_para = [slc_para]
    old_steps.inputs.parameters['slice_id'] = InputParameter()
    for idx, a_step in enumerate(old_steps.steps):
        a_step_slcs = []
        for a_slc_para in slc_para:
            if a_slc_para in a_step.inputs.parameters.keys():
                a_step_slcs.append(a_slc_para)
        if len(a_step_slcs) > 0:
            old_steps.steps[idx].inputs.parameters['slice_id'] = InputParameter()
            old_steps.steps[idx].set_parameters({'slice_id': old_steps.inputs.parameters['slice_id']})

            old_steps.steps[idx].template.inputs.parameters['slice_id'] = InputParameter()
            old_steps.steps[idx].template.inputs.parameters['slice_id'] = old_steps.steps[idx].inputs.parameters['slice_id']

            slc_dict = {a_slc_para: str(old_steps.steps[idx].template.inputs.parameters['slice_id'])
                        for a_slc_para in a_step_slcs}
            old_steps.steps[idx].template.input_parameter_slices = slc_dict
            old_steps.steps[idx].template.render_script()
    return old_steps


def unfix_steps_in_para(old_steps:Steps, unfix_para: Union[List[str], str]):
    if isinstance(unfix_para, str):
        unfix_para = [unfix_para]
    for a_para in unfix_para:
        old_steps.inputs.parameters[a_para] = InputParameter()
    for idx, a_step in enumerate(old_steps.steps):
        a_step_unfixes = []
        for a_para in unfix_para:
            if a_para in a_step.inputs.parameters.keys():
                a_step_unfixes.append(a_para)
        if len(a_step_unfixes) > 0:
            for a_para in a_step_unfixes:
                old_steps.steps[idx].set_parameters({a_para: old_steps.inputs.parameters[a_para]})
                old_steps.steps[idx].template.inputs.parameters[a_para] = old_steps.steps[idx].inputs.parameters[a_para]
    return old_steps



