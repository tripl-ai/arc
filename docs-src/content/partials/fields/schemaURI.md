Used for multiple purposes:

- Can be used to set metadata on a the extracted `DataFrame`. Note this will overwrite the existing metadata if it exists.

- Can be used to specify a schema in case of no input files. This stage will create an empty `DataFrame` with this schema so any downstream logic that depends on the columns in this dataset, e.g. `SQLTransform`, is still able to run. This feature can be used to allow deployment of business logic that depends on a dataset which has not been enabled by an upstream sending system.
