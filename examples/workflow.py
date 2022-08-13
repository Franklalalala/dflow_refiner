from dflow import (
    Workflow,
    Step,
    upload_artifact
)
from dflow.python import PythonOPTemplate
from dflow_hello import Hello

if __name__ == "__main__":
    wf = Workflow(name="hello")

    with open("foo.txt", "w") as f:
        f.write("dflow")

    artifact = upload_artifact("foo.txt")
    step = Step(
        name="step", 
        template=PythonOPTemplate(Hello, image="franklalalala/franklalalala"),
        parameters={"num": 2}, 
        artifacts={"foo": artifact},
    )
    wf.add(step)
    wf.submit()
