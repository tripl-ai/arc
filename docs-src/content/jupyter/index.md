---
title: Arc Jupyter
weight: 96
type: blog
---

[Arc Jupyter](https://github.com/tripl-ai/arc-jupyter) is a custom kernel for Jupyter Notebooks (via the plugin API) which allows users to build Arc jobs in an interactive manner. This page aims to document some of the functionality of the plugin. Due to the startup time of the Java Virtual Machine that Spark requires some of these features can take a while to become active.

<video style="padding-top: 10px; outline: none;" preload="none" src="/img/arc-notebook.mp4" poster="/img/arc-notebook.png" controls width="100%" height="100%"></video>

## Magics

Arc Jupyter provides some `magic` commands to make developing notebooks easier.

| Magic | Description |
|----------|----------|
|`%conf`|Allows setting Arc Jupyter configuration variables.|
|`%configexecute`|Shorthand for executing a [ConfigExecute](/execute/#configexecute) stage. Expects a single line configuration then a SQL statement. This can be used to generate runtime variables from data.|
|`%env`|Allows setting Arc Jupyter environment variables as key/value pairs. E.g. `ETL_CONF_BASE_DIR=/home/jovyan/tutorial` to set the `ETL_CONF_BASE_DIR` variable.|
|`%list`|Allows listing files in a directory. Expects directory to be passed in second line.|
|`%log`|Shorthand for executing a [LogExecute](/execute/#logexecute) stage. Expects a single line configuration then a SQL statement.|
|`%metadata`|Display an Arc metadata dataset for the input view.|
|`%metadatafilter`|Shorthand for executing a [MetadataFilterTransform](/transform/#metadatafiltertransform) stage. Expects a single line configuration then a SQL statement.|
|`%metadatavalidate`|Shorthand for executing a [MetadataValidate](/validate/#metadatavalidate) stage. Expects a single line configuration then a SQL statement.|
|`%schema`|Display a JSON formatted schema for the input view.|
|`%secret`|Allows entering runtime secrets which are not saved.|
|`%sql`|Shorthand for executing a [SQLTransform](/transform/#sqltransform) stage. Expects a single line configuration then a SQL statement. As views are registered (by stages like [SQLTransform](/transform/#sqltransform) they will be added to this list to rapidly generate select statements for all columns (including nested values).|
|`%sqlvalidate`|Shorthand for executing a [SQLValidate](/validate/#sqlvalidate) stage. Expects a single line configuration then a SQL statement.|
|`%version`|Print Arc Jupyter version information.|

### Example

<video style="padding-top: 10px; outline: none;" preload="none" src="/img/arc-jupyter-magics.mp4" poster="/img/arc-jupyter-magics.png" controls width="100%" height="100%"></video>

## Completer

Arc Jupyter provides `Completer` functionality to help rapidly develop jobs. This functionality relies on the Spark Kernel having started so will only work after at least one stage has executed.

To execute this start typing some letters and press the `tab` key to invoke the `Completer` functionality. The arrow keys and the `enter` button can be used to select an item. Additionally the link icon on the right of the box can be clicked to link to external documentation.

This functionality is part of the Arc plugin functionality so can be automatically added with any custom extensions you develop if the `JupyterCompleter` trait is implemented.

### Example

<video style="padding-top: 10px; outline: none;" preload="none" src="/img/arc-jupyter-completer.mp4" poster="/img/arc-jupyter-completer.png" controls width="100%" height="100%"></video>