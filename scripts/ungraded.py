#!/usr/bin/env python3

'''ungraded.py - extract answers to ungraded problems

Data is obtained from the `course_structure` and `courseware_studentmodule`
data, which are currently specified at the top of the file.

This was developed in response to a request from an instructor to obtain
individual student responses to ungraded questions *with 0 weight*. However,
upon inspection, I noted that all items had either 0 weight, or had weight
unspecified, so I extract answers for the (maybe?) more general class of *all*
ungraded problems. It's easy enough for an instructor to ignore a given file,
and once we've read the data in, that process is pretty fast - so this slightly
more general approach seems apropriate.
'''

from os import path, makedirs  # Nicer than mkdir
import json
# To interact with GnuPG
from subprocess import Popen, PIPE

import pandas as pd

# Some utility functions - they may eventually go elsewhere

# And here, we'll always expect GPG'd files now
command = 'gpg --output -'.split()


def read_encrypted_json(fname):
    print('reading JSON')
    with Popen(command + [fname],
               universal_newlines=True,
               # json.load needs strings, not bytes
               stdout=PIPE) as struct_pipe:
        return json.load(struct_pipe.stdout)


def read_encrypted_tsv(fname):
    # A sql dump that's actually a TSV file Reducing the amount of parsing &
    # data retained here is likely the biggest optimization target

    print('reading TSV')
    with Popen(command + [fname], stdout=PIPE) as student_pipe:
        # I don't know if it matters if we give pandas bytes or strings
        # For now, we are using bytes, which makes the most sense to me
        # (pandas has lots of fast parser code in C)
        return pd.read_csv(student_pipe.stdout, '\t', na_values='na',
                           # Subset for efficiency
                           usecols=['module_id', 'student_id', 'state',
                                    'created', 'modified', 'done'],
                           # Likewise, avoid parsing for efficiency
                           dtype=str)


# The meat of the script
class UngradedProblems:

    '''The set of all ungraded problems for a given course'''

    def __init__(self, database_prefix, course_name):
        # An encrypted JSON file with information about course content
        structure_fname = (
            database_prefix + course_name +
            '-course_structure-prod-analytics.json.gpg')

        self.course_structure = read_encrypted_json(structure_fname)

        student_fname = (
            database_prefix + course_name +
            '-courseware_studentmodule-prod-analytics.sql.gpg')

        self.student_df = read_encrypted_tsv(student_fname)

        self.extract_ungraded()

    def extract_student_answers(self, s):
        '''Helper for the following loop

        Student answers are unfortunately heavily quoted JSON (so you see
        things like "The following is \\"Quoted\\""), as well as ASCII-escaped
        unicode characters.'''

        # This will convert to bytes, then convert to unicode on the way back
        # in - assuming you're on Python 3 (If you're dealing with Unicode
        # hell, you definitely want to be in Python 3).

        # encode() for an ASCII string simply converts to bytes - there's no
        # real "encoding"
        s = s.encode().decode('unicode_escape')
        data = json.loads(s)

        # This appears to be where the student answers reliably occur. Of the
        # data I've looked at, this is a blob that also includes an escaped
        # version of the i4x index. I'm scared to delete it, so I leave it. It
        # looks like this:
        # i4x-BerkeleyX-GG101x-problem-db71da27320a44bdb45df31d0d801e20_2_1
        # The initial index looked like this:
        # i4x://BerkeleyX/GG101x/problem/db71da27320a44bdb45df31d0d801e20
        # Note the lack of the trailing _2_1
        return data.get('student_answers', {})

    def extract_ungraded(self):
        '''Essentially implementing Xpath with for loops.

        Maybe better to just convert to XML (or mongo).'''

        self.ungraded = {}

        for id, desc in self.course_structure.items():
            # Based on Dav's exploration, all top-level containers are
            # 'sequential' is this guaranteed to be true? I don't have
            # documentation.
            if desc['category'] == 'sequential':
                # Based on Dav's exploratons, graded 'sequential' objects have
                # 'graded': True. Ungraded objects lack this attribute.
                if 'graded' not in desc['metadata']:
                    vert_ids = desc['children']
                    for i, vid in enumerate(vert_ids):
                        self.add_from_vert(self.course_structure[vid])

    def add_from_vert(self, vert):
        '''Add problems directly into self.ungraded'''

        for child_id in vert['children']:
            child = self.course_structure[child_id]
            # There are many other categories, but I'm not sure
            # how to make sense of all of them. Some instructors
            # are interested in, e.g., seeing how much of a
            # video was played.
            if child['category'] == 'problem':
                # If you want to debug, print stuff here:
                # print('\t', child['metadata']['display_name'])
                vert_name = vert['metadata']['display_name']
                self.ungraded.setdefault(
                    vert_name, []).append(
                    (child['metadata']['display_name'], child_id))

    def write_records(self, base_dir):
        # Create a hierarchy of directories and files corresponding to sections
        # and student answers to ungraded selected problems
        for section, problems in self.ungraded.items():
            # Might not work on Windows (same with .to_csv() below)
            makedirs(path.join(base_dir, section), exist_ok=True)
            for name, pid in problems:
                curr_student_rows = self.student_df.module_id == pid
                raw_records = self.student_df.loc[curr_student_rows]
                # This triggers a warning, but we don't want to do this on all
                # rows!  We know we're potentially working on a DataFrame view
                # (but probably not).
                raw_records['student_answers'] = raw_records.state.apply(
                    self.extract_student_answers)
                outfname = 'ungraded_problems/{}/{}.tsv'.format(section, name)
                raw_records.to_csv(outfname, sep='\t')

if __name__ == '__main__':
    database_prefix = '../../database/berkeleyx-2015-02-01/'
    course_name = 'BerkeleyX-GG101x-1T2014'

    ungraded = UngradedProblems(database_prefix, course_name)
    ungraded.write_records(base_dir='ungraded_problems')
