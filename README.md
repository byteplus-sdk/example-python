## example for byteplus

#### How to run example
Take the retail industry as an example:
* clone the project.
* install requirements.
* add example root path to envs.
* enter the example directory.
* fill necessary parameters.
* run main.py.

```shell
git https://github.com/byteplus-sdk/example-python.git
cd example-python
pip3 install -r requirements.txt
export PYTHONPATH=$PYTHONPATH:$PWD
cd example/retailv2
# fill in tenant, token, tenantID and other parameters.
python3 main.py
```