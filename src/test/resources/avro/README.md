To recreate these files run:

```bash
docker run -it -v $(pwd):/data conda/miniconda3 python
```

Then:

```python
# install avro-python3 - build your docker image so this is already installed
import subprocess
subprocess.call(['pip', 'install', 'avro-python3'])

import io
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schemajson = """{"namespace": "users.avro",
  "type": "record",
  "name": "User",
  "fields": [
      {"name": "name", "type": "string"},
      {"name": "favorite_number",  "type": ["int", "null"]},
      {"name": "favorite_color", "type": ["string", "null"]}
  ]
}"""

file = open("/data/user.avsc", "w")
file.write(schemajson)
file.close()

schema = avro.schema.Parse(open("/data/user.avsc", "rb").read())

# write a full avro file with schema embedded
writer = DataFileWriter(open("/data/users.avro", "wb"), avro.io.DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

# verify it can be read (no need to specify schema as it is embedded)
reader = DataFileReader(open("/data/users.avro", "rb"), avro.io.DatumReader())
for user in reader:
  print(user)

reader.close()
      

# write a binary avro file without schema
file = open("/data/users.avrobinary", "wb")
encoder = avro.io.BinaryEncoder(file)
writer = avro.io.DatumWriter(schema)
writer.write({"name": "Alyssa", "favorite_number": 256}, encoder)
writer.write({"name": "Ben", "favorite_number": 7, "favorite_color": "red"}, encoder)
file.close()

# verify it can be read if the correct schema is known
file = open("/data/users.avrobinary", "rb")
bytes_reader = io.BytesIO(file.read())
decoder = avro.io.BinaryDecoder(bytes_reader)
reader = avro.io.DatumReader(schema)
 
while True:
  try:
    rec = reader.read(decoder)
    print(rec)
  except:
    break
```