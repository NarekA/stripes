#!/usr/bin/env python
from stripes import Stripe

"""

This example job takes the levels table:

1 prekindergarten
2 kindergarten
3 1st
4 2nd
5 3rd
6 4th
7 5th
8 6th
9 7th
10 8th
11 9th
12 10th
13 11th
14 12th
15 higher-education

and counts how many times each each suffix (last 2 letters of the name) appears.
This is the result:

en 2
nd 1
on 1
rd 1
st 1
th 9



"""


class Zebra(Stripe):

    def mapper(self, line):
        level_name = line[1]
        level_suffix = level_name[-2:]
        self.output([level_suffix, 1])


    def combiner(self, key, values):
        self.reducer(key, values)


    def reducer(self, key, values):
        self.output(key + [sum(int(i[0]) for i in values)])


if __name__ == '__main__':
    Zebra().run()
