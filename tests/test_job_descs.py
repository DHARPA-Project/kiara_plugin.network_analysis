# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.models.job import JobTest

"""Auto-generated tests that use job descriptions in the 'examples/jobs' folder and run them.

To test against the outputs of those jobs, add files into subfolders that are called the same as the job (minus the file extension), under the `tests/job_tests` folder.

To test values directly, add a file called `outputs.json` or `outputs.yaml` into that folder, containing a 'dict'
data structure with the value attribute to test as key and the expected value as value.

Most likely you will want to test against a value property, which would be done like so (in yaml):

```yaml
network_data::properties::metadata.graph_properties::number_of_self_loops: 1
```

The format is:
```yaml
<output field name>::properties::<property name>::[<property attribute>]::[<more_sub_keys>]: <expected value>
```

In case of scalars, you can also test against the value directly:
```
output_string::data: "some string"
```

If the results are too complex to test against in this way, you can also write Python code. Add a file with a random
name and a `.py` extension (output.py is a good choice if you only have one). In that file, each function will be run
against the job results. You can control which arguments will be passed to the function by naming the arguments:

- `kiara_api`: a kiara api instance will be passed in
- 'outputs`: the whole result of the job will be passed in (of type `ValueMap`)
- the field name of the value you are interested in (e.g. `table`, `network_data`, depends on the job)

Specifying any other argument name will throw an error.
"""


def test_job_desc(example_job_test: JobTest):
    example_job_test.run_tests()
