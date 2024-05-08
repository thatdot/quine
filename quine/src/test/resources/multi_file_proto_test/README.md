This folder contains supplemental data for protobuf tests where the schema is spread across multiple proto files (but compiled into a single desc file). In particular, there is an emphasis on reusing the same "short" type names (i.e., Zone) and field names to allow tests that guard against problems due to ambiguous name resolution.

To recompile the descriptor file, cd into the `schema` directory and run the following command:

```shell
./compile_schema.sh <path_to_protoc>
```

To recompile the data files, cd into the `data` directory and run the following command:

```shell
./encode_examples.sh <path_to_protoc>
```