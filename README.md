Stripes 
==============

A Framework for Writing Python Streaming jobs
--------------

stripes.py contains all the source code

*zebra.py* is an example job

The motivation for creating this framework is to streamline the process of creating a python mapreduce job.
This framework makes it so a user can do the configuration, mapper, and reducer all in one python script using much fewer lines of code.

## How to Use:

The basic structure of a job is a class that inherits from the Stripe class:

```
from edmodo.stripes import Stripe

class Zebra(Stripe):
...
```

### Mapper

Then a mapper function must be declared:

```
    def mapper(self, line):
        mapped_line = do_something_with(line)
        self.output(mapped_line)
```

Them mapper has a row passed into it as a python list, it processes the line and uses the self.output method to output the line. A mapper function can output 0, 1, or many rows for each input line. The output should be a list of values where the mapreduce partition and sort keys come first, and the valuse follow. See the config section to learn how to set the number of mapreduce keys.

If a mapper function is not declared, stripes will user it's default null mapper instead which outputs the input without altering it.

### Reducer

Besides the self.mapper method, you can also declare self.reducer and self.combiner methods. These methods are optional and if they are not declared, the job will run as a map-only job.

In vanilla python streaming, the reducer function has to iterate through sys.stdin and aggregate the valuse. Anyone who has written more than one mapreduce can tell you that this process is tedious and redundant, which is why stripes takes care of this part of the process, (see `self.run_reducer` for example). For a stripes job, the reducer is passed in a `key` and a `value` the key is a list which corresponds to the mapreduce partition/sort key and the values, which is an aggregated list of all the values which had that key. (Note that values is a list of lists, since the mapper can output more than one value per key, so for a wordcount the key value pair could be: `self.reducer(['word'], [[1], [2], [15]])`) 
An example reducer which counts the occurence of each key could look like this:

```
    def reducer(self, key, values):
        self.output(key + [sum(int(i[0]) for i in values)])
```

### Combiner
The combiner follows the same protocol as the reducer, if it is not defined, it will not run. The most common combiner is having a combiner which is the same as the reducer, the simplest way to do this is the following:

```
    def combiner(self, key, values):
        self.reducer(key, values)
```


### Aggregator
As I mentioned earlier, in vanilla streaming, one aggregates the values in the reducer for each reduce key. Stripes takes care of this process by collecting the values in a list, this is the most versatile method to aggregate the values, but it can be memory intensive and does not work for all jobs, for example if one wanted to count the number of total words in a text, there would be only one reduce key, and the values would be a very long list. Because of this, stripes allows you to overwrite the aggregator, the default aggregator is written as:

```
    def aggregator(self, old, new):
        return (old or []) + [new]
```
The aggregator takes the existing list of values (old) and appends the new value to it. The aggregator has to have a default value for the first time a key is seen. The default value lets stripes know what datatype is being used to aggregate. An example custom aggregator can be:

```
    def aggregator(self, old, new):
        return (old or 0) + int(new[0])
```
(note that new is a list since the map output can have more than one value)
This aggregator keeps a running sum of all values that correspond to the key, note that it does not have the same memory issues that the default aggregator has.

### Config
So far I have not mentiones anything about how to configure your mapreduce. The constructor has an optional config variable called `conf`. You can pass in a dictionary of config settings for your job to use, the possible settings and their defaults are:

```
conf = {
    'input_dir': '{YOUR_INPUT}',
    'output_dir': '/tmp/streaming_out',
    'history_loc': '/tmp/history',
    'priority': 'NORMAL',
    'num_partition_keys': 1,
    'combiner': None,
    'side_files': None,
    'in_sep': '\x01',
    'out_sep': '\x01',
    'allowed_errors': 0,
    'streaming_args': None,
    'p': 'launch',
    'overwrite': True,
    'reduce_tasks': 1
}
```

### Run Method
To run a job, create an instance of your class and use the `.run()` method:
```
if __name__ == '__main__':
    Zebra().run()
```

### Side Files
Side files is the protocol for loading data into the distributed cache. If you want to load files into the distributed cache you have to pass in a list in this format:

`[('filetype', filename)]`

`filetype` can be `local` or `hdfs`

`filename` is the full path to the file

In order to load the data, you must overwrite the self.load_side_files(method). If side files are declared, this method is run at the beginning of the map, reduce, and combine phases. If the data is only needed in some of the phases the side files one can use `self.conf['phase']` to only load the files in that phase.

```
    if self.conf['phase'] == 'map':
        load_files()
```


### Testing
In order to test your mapreducs, you can run `python zebra.py -p test` and this will load the first 100 lines from the input directory and:
*run them through the mapper
*sort the output
*run the results through the combiner
*sort again
*run the results through the reducer

The output will look like this:
```
--mapper output--
en      1
en      1
st      1
nd      1
rd      1
th      1
th      1
th      1
th      1
th      1
th      1
th      1
th      1
th      1
on      1

--combiner output--
en      2
nd      1
on      1
rd      1
st      1
th      9

--reducer output--
en      2
nd      1
on      1
rd      1
st      1
th      9
```

## Implementation



License
=======
Copyright 2013, Edmodo, Inc. 

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except in compliance with the License.
You may obtain a copy of the License in the LICENSE file, or at:

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Contributions
=======

We'd love for you to participate in the development of our project. Before we can accept your pull request, please sign our Individual Contributor License Agreement. It's a short form that covers our bases and makes sure you're eligible to contribute. Thank you!

http://goo.gl/gfj6Mb
