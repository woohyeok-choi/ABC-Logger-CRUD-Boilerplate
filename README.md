# ABC-Logger-CRUD-Boilerplate

## Initial Setting
* Install a package
```cmd
pip install grpcio-tools
```

* Get the ABC-Logger's gRPC specification (Suppose to store it into a directory,`grpc`) .
```cmd
git submodule add https://github.com/woohyeok-choi/ABC-Logger-gRPC-Specs grpc
```

* If the specification already is stored, then update it to the latest version.
```cmd
git submodule update --init --remote --checkout
```

* Generate python codes for protobuf and gRPC (Suppose codes are generated in src/grpc)
```cmd
python -m grpc_tools.protoc --proto_path=./grpc --python_out=./src/grpc --grpc_python_out=./src/grpc ./grpc/*.proto
``` 

* Correct importing paths of generated codes; for example, in `datum_pb2.py`
```python
# Original one
import subject_pb2 as subject__pb2

# Correction
from . import subject_pb2 as subject__pb2
```

## How-to-get Data
* Please check [this code](src/operation/operation.py)
