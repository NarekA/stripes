#!/usr/bin/env python
from stripes import Stripe
import re

conf = {
    'input_dir': 'MESSAGE_DATA_DIR',
    'output_dir': '/tmp/streaming_out',
    'side_files': [('local', 'words_to_count.txt')]
}


class Wordcount(Stripe):
    """A word count example using
    the message_data table
    """
    def __init__(self, conf):
        super(Wordcount, self).__init__(conf)
        self.aggregator = self.count_aggregator
        self.combiner = self.reducer

    def mapper(self, line):
        if line[0].strip() == '9':  # Ignore fake Edmodo user
            return None
        content = re.split('\W+', line[1].lower())
        for word in content:
            if word in self.words:   # Using data loaded from the distributed cache
                self.output([word, 1])

    def reducer(self, key, values):
        self.output(key + [values])

    def load_side_files(self, phase):
        """This is how you load data
        from the distributed cache
        """

        if phase == 'map':
            with open('words_to_count.txt') as f:
                self.words = set(word.strip() for word in f)
        

if __name__ == '__main__':
    Wordcount(conf).run()
