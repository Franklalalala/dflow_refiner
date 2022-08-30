import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union


from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from dflow_refiner.tools import steps_slc_in_para
upload_packages.append(r'C:\Users\86185\anaconda3\envs\dflow\Lib\site-packages\dflow_refiner')


class Empty(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_s': str,
            'in_s2': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_s': str
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        print(op_in['in_s2'])
        print(type(op_in['in_s2']))
        op_out = OPIO({
            "out_s": op_in['in_s2'],
        })
        return op_out

class Empty2(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_s': str,
            'in_s3': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_s': str
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        out = int(op_in['in_s']) + int(op_in['in_s3'])
        op_out = OPIO({
            "out_s": str(out),
        })
        return op_out


if __name__ == "__main__":

    # Initiation
    # ============================================================================
    a_steps = Steps(name='steps')

    a_steps.inputs.parameters['in_s'] = InputParameter()
    a_steps.inputs.parameters['in_s2'] = InputParameter()
    a_steps.inputs.parameters['in_s3'] = InputParameter()

    an_empty = Step(name='empty',
                   template=PythonOPTemplate(Empty, image="franklalalala/py_autorefiner"),
                   parameters={'in_s': a_steps.inputs.parameters['in_s'],
                               'in_s2': a_steps.inputs.parameters['in_s2']},
                   )
    a_steps.add(an_empty)

    an_empty_2 = Step(name='empty-2',
                      template=PythonOPTemplate(Empty2, image="franklalalala/py_autorefiner"),
                      parameters={'in_s': an_empty.outputs.parameters['out_s'],
                                  'in_s3': a_steps.inputs.parameters['in_s3']}
                      )
    a_steps.add(an_empty_2)
    # ===============================================================================
    a_steps = steps_slc_in_para(old_steps=a_steps, slc_para=['in_s2', 'in_s3'])

    final = Step(name='final',
                 template=a_steps,
                 parameters={'in_s': '3',
                             'in_s2': ['2', '4'],
                             'in_s3': ['10', '100'],
                             'slice_id': "{{item}}",
                             },
                 with_param=argo_range(2)
                 )

    wf = Workflow(name='steps-gym')
    wf.add(final)
    wf.submit()

