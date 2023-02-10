import shlex
import os
import pandas as pd
import numpy as np
import subprocess
import yaml


def parse_cmd_output(text):
    try:
        return [x.split("|")[1].strip() for x in text.split("\n") if x]
    except IndexError:
        return [x.strip() for x in text.split("\n") if x]


def run_shell_command(cmd_line, input_text='', debug=False):
    cmd = cmd_line.split()
    input_bytes = input_text.encode('utf-8')
    result = subprocess.run(cmd, stdout=subprocess.PIPE, input=input_bytes)
    if debug:
        print("Command: {}\nInput: {}\nResult code: {}\nOutput: {}".format(
            cmd_line, input_text,
            result.returncode, result.stdout.decode('utf-8')))
    return result.stdout.decode('utf-8')


def run_yaml_command(cmd):
    return yaml.safe_load(run_shell_command(cmd + ' -f yaml '))


@np.vectorize
def run_yaml_with_param(cmd, param, column_name):
    if not param:
        return None
    return run_yaml_command(cmd + ' ' + str(param))[column_name]


def run_df_command(cmd):
    return pd.DataFrame(pd.json_normalize(run_yaml_command(cmd)))


def flatten_object(data):
    result = {}
    for k in data:
        if isinstance(data[k], dict):
            sub = flatten_object(data[k])
            for s in sub:
                result[str(k) + '_' + s] = sub[s]
        elif isinstance(data[k], list):
            result[str(k)] = ', '.join([str(x) for x in data[k]])
        else:
            result[str(k)] = data[k]
    return result


def munch_to_dataframe(source, id_field='id'):
    result = pd.DataFrame()
    for elem in source:
        elem_id = elem[id_field]
        obj = flatten_object(elem)
        for key in obj:
            try:
                result.at[elem_id, key] = obj[key]
            except ValueError:
                result[key] = result[key].astype(object)
                result.at[elem_id, key] = obj[key]
    return result


def openrc_to_env(filename):
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content if x.startswith('export')]
    for line in content:
        var, value = shlex.split(line, posix=True)[1].split('=', 1)
        os.environ[var] = value


def read_pepperrc(filename):
    pepperrc = {}
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content if x.startswith('SALTAPI')]
    for line in content:
        var, value = line.split('=', 1)
        pepperrc[var] = value
    return pepperrc
