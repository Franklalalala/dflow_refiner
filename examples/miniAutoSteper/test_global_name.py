import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Union


from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs, OutputParameter
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices, Parameter
from dflow_refiner.tools import steps_slc_in_para
from dflow.client import v1alpha1_parameter, V1alpha1Parameter


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
                   template=PythonOPTemplate(Empty,
                                             image="python:3.8",
                                             output_parameter_global_name={'out_s': 'ss'}),
                   parameters={'in_s': a_steps.inputs.parameters['in_s'],
                               'in_s2': a_steps.inputs.parameters['in_s2']},

                   )


    a_steps.add(an_empty)

    an_empty_2 = Step(name='empty-2',
                      template=PythonOPTemplate(Empty2, image="python:3.8"),
                      parameters={'in_s': "{{workflow.outputs.parameters.ss}}",
                                  'in_s3': a_steps.inputs.parameters['in_s3']}
                      )
    a_steps.add(an_empty_2)
    # a_steps.outputs.parameters['sss'] = OutputParameter()
    # a_steps.outputs.parameters['sss'].value_from_parameter = OutputParameter(global_name='ss', type='parameter')

    # a_steps.outputs.parameters['sss'].global_name = 'ss'
    # ===============================================================================

    final = Step(name='final',
                 template=a_steps,
                 parameters={'in_s': '3',
                             'in_s2': '2',
                             'in_s3': '10',
                             },
                 )

    # a_para = Parameter(global_name='ss', type=str)

    wf = Workflow(name='steps-gym')
    wf.add(final)

    wf.submit()

    wf2 = Workflow(id='steps-gym-zgg8g')



