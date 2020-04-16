---
title: Getting started
weight: 10
type: blog
---

## Get Started

To quickly get started with a real-world example you can clone the [Arc Starter](https://github.com/tripl-ai/arc-starter) project which has included job definitions and includes a limited [set of data](https://registry.opendata.aws/nyc-tlc-trip-records-pds/) for you to quickly try Arc in a [custom](https://github.com/tripl-ai/arc-jupyter) [Jupyter Notebooks](https://jupyter.org/) environment.

```bash
git clone https://github.com/tripl-ai/arc-starter.git
cd arc-starter
./develop.sh
```

The example is within the `examples/0/` directory.

To work through a complete example try completing the [tutorial](/tutorial).

## Notebook

Arc provides an interactive development experience via a [custom](https://github.com/tripl-ai/arc-jupyter) [Jupyter Notebooks](https://jupyter.org/) extension. This has been bundled and is available as a Docker image: https://hub.docker.com/r/triplai/arc-jupyter

```bash
docker run \
--name arc-jupyter \
--rm \
-e JAVA_OPTS="-Xmx4096m" \
-p 4040:4040 \
-p 8888:8888 \
{{% arc_jupyter_docker_image %}} \
start-notebook.sh \
--NotebookApp.password='' \
--NotebookApp.token=''
```

![Notebook](/img/arc-starter.png)

## Examples

Example data processing pipelines are available in the [Arc Starter](https://github.com/tripl-ai/arc-starter/tree/master/examples) project in the [examples](https://github.com/tripl-ai/arc-starter/tree/master/examples) directory.
