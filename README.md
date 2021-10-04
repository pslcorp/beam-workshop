# Apache Beam Workshop

**Beam** 101 workshop using **Python** _(and the **Spark** runner)_.

## Prerequisites

+ **Python 3** _(3.6, 3.7 or 3.8 are supported as the day of writing)_
  + **pip**
  + **venv**
+ **Docker** _(for running a standalone **Spark** cluster)_

## Installation

```bash
# First create and activate a virtual env
python3 -m venv .venv
source .venv/bin/activate

# Then install the dependencies of this project inside the virtual env
python3 -m pip install wheel
python3 -m pip install -r requirements.txt

# Finally download the beam_spark_job_server docker image so we can use it latter
docker pull apache/beam_spark_job_server
```

# Basics

**Beam** is a high-level framework to create data-parallel processing pipelines
that can either be batch or streaming.<br>
The pipelines can be defined using any of the available SDKS _(**Python**, **Java**, **GO**)_,
and executed on multiple different execution engines, like: **Dataflow**, **Spark** & **Flink**.

**Beam** is particularly useful for embarrassingly parallel data processing tasks,
in which the problem can be decomposed into many smaller bundles of data
that can be processed independently and in parallel.
For example, ETL tasks and pure data integration jobs.

## PCollection

The main abstraction provided by **Beam** are the `PCollections`.<br>
A `PCollection` represents a potentially distributed, multi-element data set,
they _"contain"_ the data of the pipeline and are you only mechanism to interact / transform it.<br>
They can contain any type of data, but all elements of the same `PCollection` must be of the same type.<br>
They are created by a pipeline and belong to such pipeline,
thus you can not share `PCollections` between different pipelines.

`PCollections` are immutable. Once created, you cannot add, remove, or change individual elements.
You might process each element of a `PCollection` and generate new one,
but it does not consume or modify the original input collection.(*)

> **Beam** avoids unnecessary copying of elements.
> Thus `PCollection` contents are logically immutable, not physically immutable.
> Changes to input elements may be visible to other `DoFns` executing within the same bundle,
> and may cause correctness issues.<br>
> As a rule, it’s not safe to modify values provided to a `DoFn`.

A `PCollection` can be either bounded or unbounded in size.<br>
A bounded `PCollection` represents a data set of a known, fixed size.
While an unbounded `PCollection` represents a data set of unlimited size.
Whether a `PCollection` is bounded or unbounded depends on the source of the data set that it represents.<br>
**Beam** uses windowing to divide a continuously updating unbounded `PCollection`
into logical windows of finite size.
Aggregation transforms _(such as `GroupByKey` & `Combine`)_ work on a per-window basis.

> For a complete catalog of all transformations check [this](https://beam.apache.org/documentation/transforms/python/overview/).

## Side inputs / Calculated values

In addition to the main input _(the `PCollection`)_,
you can provide additional inputs to a transformation in the form of side inputs.<br>
A side input is an additional input that your transformation can access
each time it processes an element in the input `PCollection`.<br>
Such values might be determined by a different branch of your pipeline.

A windowed `PCollection` may be infinite and thus cannot be compressed
into a single value _(or single collection class)_.
When you create a `PCollectionView` of a windowed `PCollection`,
the `PCollectionView` represents a single entity per window.

# Unit testing

The approach to unit test **Beam** pipelines is to separate
all the logic _(data transformations)_ from the input and output.<br>
Then, you can use the `TestPipeline` and `beam.Create`
to create an initial `PCollection` from a small & fixed set of data.
Subsequently you will pipe your logic to this `PCollection`,
to finally use auxiliary functions **beam** provides _(like `assert_that` & `equal_to`)_
in order to validate the logic produces the expected results.

You can mix that with whatever testing framework you are using;
for example, using **unittest** you can execute the test like this:

```bash
python3 -m unittest -v test_word_count.py
```

# Spark runner

In order to run our **Beam** pipeline on a **Spark** cluster,
all we need to do is change the pipeline options;
your business logic remains the same!

The changes are:<br>
Set the `runner` to `PortableRunner`<br>
Configure the `job_endpoint` _(we will use **Docker** latter to run this)_<br>
Finally, select the `environment_type`; for testing we will be using `LOOPBACK`

```python
beam_options = PipelineOptions(
  runner = 'PortableRunner',
  job_endpoint = 'localhost:8099',
  environment_type = 'LOOPBACK'
)
with beam.Pipeline(options = beam_options) as pipeline:
```

Then let's start a **Spark** job server using **Docker**.

```bash
docker run -d --net=host apache/beam_spark_job_server
```

Now we can just run the **python** script as before.

> **Note**: For production code you should use `spark-submit`
> in conjunction with the previous setup to properly launch your job into a **Spark** cluster.<br>
> Additionally you would need to configure and use a different `environment_type`, check:
> https://beam.apache.org/documentation/runtime/sdk-harness-config/

# Extras

+ **Beam Execution Model**: Dive deep into how **Beam** pipelines are executed - https://beam.apache.org/documentation/runtime/model/
+ **Beam SQL**: A module that allow users to use **SQL** to manipulate `PCollections` - https://beam.apache.org/documentation/dsls/sql/overview/
+ **Beam DataFrames**: A DSL that allows user to manipulate `PCollections` as **Pandas** `DataFrames` - https://beam.apache.org/documentation/dsls/dataframes/overview/
+ **Beam Python Streaming**: Learn how to modify your batch code to become streaming - https://beam.apache.org/documentation/sdks/python-streaming/
+ **Beam Python Type Hints**: Allow Beam to typecheck your pipeline before running it - https://beam.apache.org/documentation/sdks/python-type-safety/
+ **Dataflow**: The original platform for which **Beam** was created - https://cloud.google.com/dataflow
+ **Scio**: A **Scala** API for **Beam** - https://github.com/spotify/scio
