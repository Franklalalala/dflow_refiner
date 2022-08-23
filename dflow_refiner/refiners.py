from typing import Dict

from dflow import Step, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import PythonOPTemplate
from dflow_refiner.build import *
from dflow_refiner.calculators import *
from dflow_refiner.inputGen import *
from dflow_refiner.parser import *


class Fixed_in_ref():
    def __init__(self,
                 cmd_list: List[str],
                 out_list: List[str],
                 in_fmt: str = None,
                 log_file: str = None,

                 is_batch_exe: bool = False,
                 num_worker: int = None,
                 cpu_per_worker: int = None,
                 base_node: int = 0,
                 poll_interval: float = 0.5,

                 cutoff_mode: str = 'None',
                 cutoff_rank: int = None,
                 cutoff_value: Union[float, int] = None,

                 is_aux_mixed: bool = False,
                 is_aux_public: bool = True,
                 pvt_aux_list: List[str] = None,
                 pbc_aux_name: str = None,

                 ctm_para: dict = dict()):
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

            ctm_para: Customized parameters
        """
        self.exe_core = {'cmd_list': cmd_list, 'out_list': out_list, 'in_fmt': in_fmt, 'log_file': log_file}
        if is_batch_exe:
            assert isinstance(num_worker * cpu_per_worker * base_node,
                              int), f'Please input batch execution related parameters.'
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

        self.ctm = ctm_para


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
        super(xTB_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs,
                                          outputs=outputs,
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
            template=PythonOPTemplate(xtbParser, image=image,
                                      output_artifact_global_name={'info': 'global_xtb_info',
                                                                   'out_cooked': 'global_xtb_out_cooked'}),
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
        super(ABC_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs,
                                          outputs=outputs,
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
            template=PythonOPTemplate(abcParser, image=image,
                                      output_artifact_global_name={'info': 'global_abc_info',
                                                                   'out_cooked': 'global_abc_out_cooked'}),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            parameters={'LM_folder': in_para.ctm['LM_folder_name'], 'prefix': self.prefix},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']


class Gau_Refiner(Refiner):
    def __init__(self, in_para: Fixed_in_ref,
                 image: str, executor: DispatcherExecutor,
                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(Gau_Refiner, self).__init__(in_para=in_para, image=image, executor=executor, name=name, inputs=inputs,
                                          outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        # Parameters could be sliced.
        self.inputs.parameters['cmd_line'] = InputParameter()
        self.inputs.parameters['charge'] = InputParameter()
        self.inputs.parameters['multi'] = InputParameter()
        self.inputs.parameters['prefix'] = InputParameter()
        self.prefix = self.inputs.parameters['prefix']

        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(gaussInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init'], 'info': self.inputs.artifacts['info']},
            parameters={'cutoff': in_para.cutoff,
                        'cmd_line': self.inputs.parameters['cmd_line'],
                        'charge': self.inputs.parameters['charge'],
                        'multi': self.inputs.parameters['multi'],
                        'cpu_per_worker': in_para.exe_batch['cpu_per_worker']
                        },
            key=f"{self.prefix}-inputGen",
        )
        self.add(step_inputGen)

        if 'has_chk_input' in in_para.ctm.keys() and in_para.ctm['has_chk_input'] == True:
            self.inputs.artifacts['in_chk'] = InputArtifact()
            step_build = Step(
                name="build",
                template=PythonOPTemplate(BuildWithAux, image=image),
                artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw'],
                           'in_aux': self.inputs.artifacts['in_chk']},
                parameters={'aux_para': in_para.aux},
                key=f"{self.prefix}-build",
            )
        else:
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

        if 'keep_chk' in in_para.ctm.keys() and in_para.ctm['keep_chk'] == True:
            self.outputs.artifacts['cooked_chk'] = OutputArtifact()
            step_parse = Step(
                name='parse',
                template=PythonOPTemplate(gaussParser, image=image,
                                          output_artifact_global_name={'info': 'global_gau_info',
                                                                       'out_cooked': 'global_gau_out_cooked',
                                                                       'cooked_chk': 'global_gau_cooked_chk'}),
                artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
                parameters={'prefix': self.prefix, 'keep_chk': True},
                key=f'{self.prefix}-parse',
            )
            self.add(step_parse)
            self.outputs.artifacts['cooked_chk']._from = step_parse.outputs.artifacts['cooked_chk']
        else:
            step_parse = Step(
                name='parse',
                template=PythonOPTemplate(gaussParser, image=image,
                                          output_artifact_global_name={'info': 'global_gau_info',
                                                                       'out_cooked': 'global_gau_out_cooked'}),
                artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
                parameters={'prefix': self.prefix, 'keep_chk': False},
                key=f'{self.prefix}-parse',
            )
            self.add(step_parse)
        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']
