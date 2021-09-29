#!/usr/bin/env python

import os
import sys
import re
import cftime
import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape
import traceback

_help = '''Usage:
   jdataset.py [options]  DATAFRAME...

Options:
    --dest DESTINATION              Destination of the generated datasets.
    -g, --groupby GROUP             Comma separated names of columns to groupby.
    -o KEY=VALUE                    Each KEY=VALUE will be available to the template as variable KEY.

    -t, --template TEMPLATE         Template to use.
'''

# filters
def f_values(series, sep=None):
    if sep is None:
        sep = ' '

    return series.apply(lambda a: sep.join(np.ravel(a).astype(str)))

def f_timeunitschange(df, timecol=None, units=None, calendar=None):
    if timecol is None:
        timecol = 'time'
    if units is None:
        units = df[(timecol, 'units')].iloc[0]
    if calendar is None:
        calendar = df[(timecol, 'calendar')].iloc[0]

    df[(timecol, '_values')] = df.apply(lambda row:
        cftime.num2date(row[(timecol, '_values')], row[(timecol, 'units')], row[(timecol, 'calendar')]), axis=1)
    df[(timecol, '_values')] = df.apply(lambda row:
        cftime.date2num(row[(timecol, '_values')], units, calendar), axis=1)

#    df[(timecol, 'units')] = units
#    df[(timecol, 'calendar')] = calendar

    return df

# non filter functions
def setup_jinja(templates):
    default_templates = os.path.join(os.path.dirname(__file__), 'templates')
    loader = ChoiceLoader([
        FileSystemLoader(templates),
        FileSystemLoader(os.getcwd()),
        FileSystemLoader(default_templates),
    ])

    env = Environment(
        loader=loader,
        autoescape=select_autoescape(['xml']),
        trim_blocks=True,
        lstrip_blocks=True)

    env.filters['basename'] = lambda path: os.path.basename(path)
    env.filters['dirname'] = lambda path: os.path.dirname(path)
    env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)

    env.filters['_values'] = f_values
    env.filters['timeunitschange'] = f_timeunitschange

    env.tests['isncml'] = lambda dataset: dataset['ext'] == ".ncml"
    env.tests['isnc'] = lambda dataset: dataset['ext'] != ".ncml"
    env.tests['onestep'] = lambda arr: len(np.unique(np.diff(arr))) == 1

    return env

def render(df, dest, **kwargs):
    try:
        d = dict(df['GLOBALS'].iloc[0])
        path = os.path.abspath(args['dest'].format(**d))

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w+') as fh:
            fh.write(template.render({**kwargs, 'df': df}))

        print(path)
    except Exception as err:
        print('Error: Could not render NcML {0}, caused by {1}'.format(path, err), file=sys.stderr)
        traceback.print_exc()

if __name__ == '__main__':
    current_abspath = os.path.abspath(os.getcwd())
    args = {
        'dest': os.path.join(current_abspath, 'unnamed.ncml'),
        'template': 'basic.ncml.j2',
        'opts': {},
        'dfs': [],
        'groupby': None,
    }

    arguments = len(sys.argv) - 1
    position = 1
    while arguments >= position:
        if sys.argv[position] == '-h' or sys.argv[position] == '--help':
            print(_help)
            sys.exit(1)
        elif sys.argv[position] == '-t' or sys.argv[position] == '--template':
            args['template'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '-d' or sys.argv[position] == '--dest':
            args['dest'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '-o':
            opt = sys.argv[position+1].split('=')
            opt_key = opt[0]
            opt_value = '='.join(opt[1:])
            args['opts'][opt_key] = opt_value
            position+=2
        elif sys.argv[position] == '-g' or sys.argv[position] == '--groupby':
            groups = sys.argv[position+1].split(',')
            args['groupby'] = [('GLOBALS', g) for g in groups]
            position+=2
        else:
            args['dfs'].append(sys.argv[position])
            position+=1

    if len(args['dfs']) < 1:
        print(_help)
        sys.exit(1)

    template_abs_path = os.path.abspath(args['template'])
    env = setup_jinja(os.path.dirname(template_abs_path))
    template = env.get_template(os.path.basename(args['template']))

    for arg_df in args['dfs']:
        if not os.path.isfile(arg_df):
            print('Non existing file: %s. Exiting...' % arg_df, file=sys.stderr)
            sys.exit(1)

    for arg_df in args['dfs']:
        df = pd.read_pickle(arg_df)
        if args['groupby'] is not None:
            for _,g in df.groupby(args['groupby']):
                render(g, args['dest'], **args['opts'])
        else:
            render(df, args['dest'], **args['opts'])
