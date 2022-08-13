from pathlib import Path
from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
)

class Hello(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'num' : int,
            'foo' : Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'num' : int,
            'bar' : Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in : OPIO,
    ) -> OPIO:
        with open(op_in["foo"], "r") as f:
            content = f.read()
        with open("output.txt", "w") as f:
            f.write("Hello " + content)

        return OPIO({
            "num": op_in["num"] * 2, 
            "bar": Path("output.txt"),
        })
