#!/usr/bin/env python
import sys
import traceback
import argparse
from subprocess import PIPE, Popen
import os
import glob
import StringIO


desc = """

A Lightweight Map Reduce Framework for Hadoop Cluster 

TODO:

- make a dry run option
- add parser method for mapper and reducer
- mapred.job.tracker=local for test method
- add unit testing
- seperate hdfs sidefiles
- seperate streaming options from commandline options
- finish documentation
- make map input seperator changable
- make number of test rows adjustable
- add filter method

"""




def run_subprocess(bash_command):
    proc = Popen(bash_command, shell=False, stdin=PIPE, stdout=PIPE)
    output, error = proc.communicate()
    return output.strip()


def get_hadoop_home():
    bash_command = ['which', 'hadoop']
    hadoop_home = run_subprocess(bash_command)
    bash_command = ['readlink', '-f', hadoop_home]
    hadoop_home = run_subprocess(bash_command)
    hadoop_home = hadoop_home.replace('bin/hadoop', 'lib/hadoop-0.20-mapreduce').strip().rstrip('/')
    return hadoop_home


def get_java_home():
    return os.environ["JAVA_HOME"] or "/usr/java/default"


def get_jar(hadoop_home):
    print hadoop_home
    return glob.glob(hadoop_home + '/contrib/streaming/*streaming.jar')[0]


def output_test(phase_name, outIO, sep):
    print '--{0} output--'.format(phase_name)
    print outIO.getvalue().replace('\x01', '\t')
    data = sorted(outIO.getvalue().strip().split('\n'))
    outIO.truncate(0)
    return data


def shorten_output(output, length=100):
    data = []
    counter = 0
    for line in output.stdout:
        if counter > length:
            output.stdout.close()
            break
        else:
            data.append(line)
            counter += 1
    return data


class Stripe(object):
    """Streaming Made Easy"""
    def __init__(self, conf=None):
        self.errors = 0
        self.conf = {
            'input_dir': '{SET DEFAULT DIRECTORY FOR YOUR CLUSTER}',
            'output_dir': '/tmp/streaming_out',
            'history_loc': '/tmp/history',
            'priority': 'NORMAL',
            'num_partition_keys': 1,
            'side_files': None,
            'in_sep': '\x01',
            'out_sep': '\x01',
            'allowed_errors': 0,
            'streaming_args': None,
            'overwrite': True,
            'reduce_tasks': 1
        }
        self.out = sys.stdout
        
        if conf:
            for k, v in conf.iteritems():
                self.conf[k] = v


    def parse_args(self):
        parser = argparse.ArgumentParser(description=desc)
        parser.add_argument('-p', '--phase', default='launch',
                type=str, help='Indicates whether to launch job, map, combine or reduce.')
        args = parser.parse_args()
        
        for k, v in args.__dict__.iteritems():
            self.conf[k] = v


    def run(self):
        cmd_conf = self.parse_args()

        run_case = {
            'launch': self.launch_job,
            'map': self.run_mapper,
            'combine': self.run_combiner,
            'reduce': self.run_reducer,
            'test': self.cat_test
        }

        run_case[self.conf['phase']]()


    def launch_job(self):
        conf = self.conf
        hadoop_home = get_hadoop_home()
        java_home = get_java_home()
        jar = get_jar(hadoop_home)
        filename = sys.modules[self.__class__.__module__].__file__
        basename = filename.split('/')[-1]
        if repr(conf['in_sep']).startswith("'\\x0"):
            ord_sep = '\\\\' + str(ord(conf['in_sep'])).zfill(3) # convert \x01 format to \\001 format

        if conf['overwrite']:  # Not sure if it works without this
            print 'removing:', conf['output_dir']
            run_subprocess(['hdfs', 'dfs', '-rm', '-r', conf['output_dir']])

        bash_command = [
            'hadoop', 'jar', jar, 
            '-D', 'hadoop.job.history.user.location={0}'.format(conf['history_loc']), 
            '-D', "stream.map.output.field.separator='{0}'".format(ord_sep),  #TODO make this seperator changable
            '-D', 'mapred.reduce.tasks={0}'.format(conf['reduce_tasks']), 
            '-D', 'mapred.job.priority={0}'.format(conf['priority']),
            '-D', 'num.key.fields.for.partition={0}'.format(conf['num_partition_keys']),
            '-input', conf['input_dir'],
            '-output', conf['output_dir'],
            '-mapper', '"python {0} -p map"'.format(basename),
            '-file', filename,
            '-file', __file__.replace('.pyc', '.py')
        ]

        if getattr(self, 'combiner', None):
            bash_command += ['-combiner', '"python {0} -p combine"'.format(basename)]

        if getattr(self, 'reducer', None):
            bash_command += ['-reducer', '"python {0} -p reduce"'.format(basename)]

        if self.conf['streaming_args']:
            bash_command += self.conf['streaming_args']

        if conf['side_files']:
            for f in conf['side_files']:
                if f[0] == 'local':
                    bash_command += ['-file', f[1]]

        print ' '.join(bash_command)
        run_subprocess(bash_command)


    def load_side_files(self, phase):
        pass

    def mapper(self, line):
        return line


    def run_mapper(self, iterator=sys.stdin):
        self.load_side_files('map')
        for line in iterator:
            try:
                line = line.strip().split(self.conf['in_sep'])
                self.mapper(line)
            except:
                sys.stderr.write('--mapper skipped--' + str(line))
                sys.stderr.write('-' + traceback.format_exc())
                self.errors += 1
                assert self.errors <= self.conf['allowed_errors']


    def aggregator(self, old, new):
        return (old or []) + [new]

    def count_aggregator(self, old, new):
        return (old or 0) + int(new[0])


    def run_combiner(self, iterator=sys.stdin):
        self.run_reducer(reduce_func=self.combiner, iterator=iterator)


    def output(self, line):
        print >> self.out, self.conf['out_sep'].join(str(i) for i in line)


    def run_reducer(self, reduce_func=None, iterator=sys.stdin):
        reduce_func = reduce_func or self.reducer
        last_tuple = None
        holder = None
        self.load_side_files('reducer')
        pindx = self.conf['num_partition_keys']

        for line in iterator:
            try:
                line = line.strip().split(self.conf['out_sep'])
                cur_tuple = line[:pindx]
                value = line[pindx:]

                if cur_tuple == last_tuple:
                    holder = self.aggregator(holder, value)
                else:
                    if last_tuple:
                        reduce_func(last_tuple, holder)
                    holder = self.aggregator(None, value)
                last_tuple = cur_tuple
            except:
                sys.stderr.write('--reducer skipped--' + repr(line))
                sys.stderr.write('-' + traceback.format_exc())
                self.errors += 1
                assert self.errors <= self.conf['allowed_errors']

        reduce_func(last_tuple, holder)


    def cat_test(self):
        from edmodo.snakebite_utils import connect_to_hdfs
        client = connect_to_hdfs()
        files = [f['path'] for f in client.ls([self.conf['input_dir']]) if f['path'].split('/')[-1][0] != '_']
        f = files[0]
        p = Popen(["hdfs", "dfs", "-text", f], stdin=PIPE, stdout=PIPE, bufsize=1)
        data = shorten_output(p)

        self.out = StringIO.StringIO()
        self.run_mapper(iterator=data)
        data = output_test('mapper', self.out, self.conf['out_sep'])

        if getattr(self, 'combiner', None):
            self.run_combiner(iterator=data)
            data = output_test('combiner', self.out, self.conf['out_sep'])

        self.run_reducer(iterator=data)
        data = output_test('reducer', self.out, self.conf['out_sep'])


if __name__ == '__main__':
    Stripe().run()
