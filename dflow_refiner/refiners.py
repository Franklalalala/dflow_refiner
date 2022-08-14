from typing import List, Union, Dict

from dflow_refiner.build import *

from dflow_refiner.calculators import *
from dflow_refiner.inputGen import *
from dflow_refiner.parser import *
from dflow import Step, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import PythonOPTemplate, BigParameter


class Fixed_in_ref():
    def __init__(self,
                 cmd_list: List[str],
                 out_list: List[str],
                 in_fmt: str = None,
                 log_file: str = None,

                 is_batch_exe: bool = False,
                 num_worker: int = None,
                 cpu_per_worker: int = None,
                 base_node: int = None,
                 poll_interval: float = None,

                 cutoff_mode: str = 'None',
                 cutoff_rank: int = None,
                 cutoff_value: Union[float, int] = None,

                 is_aux_mixed: bool = False,
                 is_aux_public: bool = True,
                 pvt_aux_list: List[str] = None,
                 pbc_aux_name: str = None,

                 parse_para: dict = None,
                 slice_para: dict = None):
        """
        Fixed refiner input parameters

        Args:
            Core execute parameters:
            cmd_list: shell command
            out_list: output files to be preserved in execute step, could be output format
            in_fmt: input file formats, if none, will take os.listdir(workbase)[0] as input
            log_file: execute logs. If none, will be no logs.

            Batch style parameters:
            is_batch_exe: flag to check batch parameter
            num_worker: number of parallel threads
            cpu_per_worker: CPU resources for each thread
            base_node: the node to be started
            poll_interval: poke intervals, if none, will continually poke

            Cutoff parameters:
            cutoff_mode: switches
            cutoff_rank: isomers below this rank will be retained
            cutoff_value: isomers have a smaller relative energy(eV) will be retained

            Build with Auxiliry related parameters:
            Public files will be repeated in every workbase, private files are identical to input files but with different formats.
            is_aux_mixed: if true, public files should be in the folder 'pbc_aux_name', private files should be isolated in different private folders.
            pbc_aux_name: see above
            pvt_aux_list: if there are multiple private folders, put folder names here.
            is_aux_public: if not mixed, flag to indicate the only auxilary folder

            parse_para: customized parse parameter.

            slice_para: for slice feature
        """
        self.exe_core = {'cmd_list': cmd_list, 'out_list': out_list, 'in_fmt': in_fmt, 'log_file': log_file}
        if is_batch_exe:
            assert isinstance(num_worker*cpu_per_worker*base_node, int), f'Please input batch execution related parameters.'
            self.exe_batch = {'base_node': base_node, 'cpu_per_worker': cpu_per_worker,
                              'num_worker': num_worker, 'poll_interval': poll_interval}

        if cutoff_mode:
            if 'rank' in cutoff_mode:
                assert cutoff_rank != None, f'Please input a cutoff rank, since the cutoff mode is {cutoff_mode}.'
            if 'value' in cutoff_mode:
                assert cutoff_value != None, f'Please input a cutoff value, since the cutoff mode is {cutoff_mode}.'
        self.cutoff = {'value': cutoff_value, 'rank': cutoff_rank, 'mode': cutoff_mode}

        if is_aux_mixed:
            assert pbc_aux_name != None, f'Please input a public floder name.'
            assert len(pvt_aux_list) != 0, f'More than one private folders required.'
        self.aux = {'is_aux_mixed': is_aux_mixed, 'is_aux_public': is_aux_public,
                    'pvt_aux_list': pvt_aux_list, 'pbc_aux_name': pbc_aux_name}

        # Customized parameters
        self.parse_para = parse_para
        self.slice_para = slice_para


class Refiner(Steps):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor,
                 name: str = None,
                 inputs: Inputs = None,
                 outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None,
                 memoize_key: str = None,
                 annotations: Dict[str, str] = None,
                 ):
        super(Refiner, self).__init__(name=name, inputs=inputs, outputs=outputs,
                                      memoize_key=memoize_key, annotations=annotations, steps=steps)
        self.in_para = in_para
        self.inputs.artifacts['init'] = InputArtifact()
        self.inputs.artifacts['info'] = InputArtifact(optional=True)

        self.outputs.artifacts['out_cooked'] = OutputArtifact()
        self.outputs.artifacts['info'] = OutputArtifact()


class xTB_Refiner(Refiner):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor,
                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(xTB_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs, outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.prefix = self.name
        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(xtbInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init'], 'info': self.inputs.artifacts['info']},
            parameters={'cutoff': in_para.cutoff},
            key=f"{self.prefix}-inputGen",
        )
        self.add(step_inputGen)

        step_build = Step(
            name="build",
            template=PythonOPTemplate(simpleBuild, image=image),
            artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw']},
            key=f"{self.prefix}-build",
        )

        self.add(step_build)

        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'core_para': in_para.exe_core, 'batch_para': in_para.exe_batch},
            executor=executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        step_parse = Step(
            name='parse',
            template=PythonOPTemplate(xtbParser, image=image),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']


class ABC_Refiner(Refiner):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor,
                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(xTB_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs, outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.inputs.artifacts['public_inp'] = InputArtifact()
        self.prefix = self.name
        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(op_class=abcInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init'], 'public_inp': self.inputs.artifacts['public_inp']},
            key=f'{self.prefix}-inputGen'
        )
        self.add(step_inputGen)

        step_build = Step(
            name="build",
            template=PythonOPTemplate(BuildWithAux, image=image),
            artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw'],
                       'in_aux': self.inputs.artifacts['init']},
            parameters={'aux_para': in_para.aux},
            key=f"{self.prefix}-build",
        )

        self.add(step_build)

        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'core_para': in_para.exe_core, 'batch_para': in_para.exe_batch},
            executor=executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        step_parse = Step(
            name='parse',
            template=PythonOPTemplate(abcParser, image=image),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            parameters={'LM_folder': in_para.parse_para['LM_folder'], 'prefix': self.prefix},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']
