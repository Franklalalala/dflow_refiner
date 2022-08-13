from pathlib import Path
from dflow.python import OPIO
from dflow_hello import Hello

def test_hello():
    hello = Hello()
    op_in = OPIO({
        "num": 2,
        "foo": Path(__file__).parent / "data" / "foo.txt"
    })
    op_out = hello.execute(op_in)
    assert op_out["num"] == 4
    with open(op_out["bar"], "r") as f:
        content = f.read()
    assert content == "Hello dflow"