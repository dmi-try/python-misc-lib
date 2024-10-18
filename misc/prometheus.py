import json
import yaml
import requests
import urllib
import pandas as pd
from collections import defaultdict

from pathlib import Path
import sys

auth_data = {}
auth_tokens = defaultdict(str)
api_debug = False

query_tmpl = "{url}/api/v1/query?query={q}"
query_range_tmpl = "{url}/api/v1/query_range?query={q}&start={start}&end={end}&step={step}"
metrics_tmpl = "{url}/api/v1/label/__name__/values"
# metrics_tmpl = "{url}/api/v1/targets/metadata"


def dates_range(period='1w', step='1h', start=None):
    if start is None:
        start = pd.Timestamp('now') - pd.Timedelta(period)
    return {'period': pd.Timedelta(period), 'step': pd.Timedelta(step), 'start': pd.Timestamp(start)}


def request_data(cloud, url):
    if cloud in auth_tokens.keys():
        req_headers = {"Authorization": f"Bearer {auth_tokens[cloud]}"}
    else:
        req_headers = {}
    if api_debug:
        print(url)
    r = requests.get(url, headers=req_headers, verify=auth_data[cloud]['verify_cert'])
    if r.url.startswith('https://keycloak'):
        keycloak_url = r.url[:r.url.find('auth?')] + 'token'
        client_id = urllib.parse.parse_qs(urllib.parse.urlparse(r.url).query)['client_id']
        data = {
            "username": auth_data[cloud]['keystone_auth']['username'],
            "password": auth_data[cloud]['keystone_auth']['password'],
            "client_id": client_id,
            "grant_type": "password"
        }
        keycloak_resp = requests.post(keycloak_url, data=data)
        token = json.loads(keycloak_resp.content)['access_token']
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    return json.loads(r.content)


def q(query, cloud, period=None, step='1h', start=None, output_format='json', metric=None):
    if period:
        if type(period) == dict:
            start = period['start']
            step = period['step']
            period = period['period']
        if start:
            date_start = pd.Timestamp(start)
        else:
            date_start = pd.Timestamp('now') - pd.Timedelta(period)
        date_end = date_start + pd.Timedelta(period)
        date_step = pd.Timedelta(step)
        url = query_range_tmpl.format(
            url=auth_data[cloud]['url'], q=urllib.parse.quote(query),
            start=date_start.timestamp(),
            end=date_end.timestamp(),
            step=date_step.total_seconds()
        )
    else:
        url = query_tmpl.format(url=auth_data[cloud]['url'], q=urllib.parse.quote(query))
    data = request_data(cloud, url)
    if output_format == 'json':
        return data
    if output_format == 'df':
        return data_to_df(data, column_name_field=metric)
    raise "Unknown output format"


def data_to_df(data, column_name_field=None, raw_data=False):
    def serialize_data(data, name):
        (i, d) = zip(*data)
        return pd.Series(pd.to_numeric(d), index=pd.to_datetime(pd.to_numeric(i) * 1000 ** 3), name=name)

    columns_data = []
    df_metric = pd.DataFrame()

    if data['status'] == 'success':
        if data['data']['resultType'] == 'vector':
            for c in data['data']['result']:
                if not column_name_field:
                    column_name = 'result'
                else:
                    try:
                        column_name = c['metric'][column_name_field]
                    except KeyError:
                        print(c['metric'].keys())
                        raise
                columns_data.append(serialize_data([c['value']], name=column_name))
                if raw_data:
                    df_metric[column_name] = pd.Series(c['metric'])
        if data['data']['resultType'] == 'matrix':
            for c in data['data']['result']:
                if not column_name_field:
                    column_name = 'result'
                else:
                    try:
                        column_name = c['metric'][column_name_field]
                    except KeyError:
                        print("Not found metric {} in {}, using 'result'".format(column_name_field, c['metric'].keys()))
                        column_name = 'result'
                columns_data.append(serialize_data(c['values'], column_name))
                if raw_data:
                    df_metric[column_name] = pd.Series(c['metric'])
    df_result = pd.DataFrame(columns_data).T
    if raw_data:
        return [df_result, df_metric]
    return df_result


def init(auth_file, selenium_path=None):
    with open(auth_file) as f:
        globals()['auth_data'] = yaml.safe_load(f)
    for c in auth_data.keys():
        if 'verify_cert' not in auth_data[c]:
            auth_data[c]['verify_cert'] = True
        print('ping', c, '-', q('1', c)['status'])


def get_metrics(cloud):
    url = metrics_tmpl.format(url=auth_data[cloud]['url'])
    data = request_data(cloud, url)
    return data


QUERIES = {
    'EU_OVERLOADED_NODES': '(avg(quantile_over_time(0.8, node_load15[2w])) by (node)) / (sum(label_replace(openstack_nova_vcpus, "node", "$1", "hostname", "(.*)")) by (node)) > 1.2',
    'US_OVERLOADED_NODES': 'avg(quantile_over_time (0.8, system_load15{host=~"cmp.*"}[2w])) by (host) / sum(label_replace(openstack_nova_vcpus, "host", "$1", "hostname", "(.*)")) by (host) > 0.8',
    'EU_NODES_RAM_ALLOC': 'sum(openstack_nova_ram - openstack_nova_free_ram) / sum(openstack_nova_ram)',
    'EU_NODES_RAM_USAGE': 'sum(node_memory_MemTotal_bytes - node_memory_MemFree_bytes - node_memory_Buffers_bytes - node_memory_Cached_bytes)/1024/1024 / sum(openstack_nova_ram)',
    'EU_NODES_CPU_ALLOC': 'sum(openstack_nova_used_vcpus) / sum(openstack_nova_vcpus)',
    'EU_NODES_CPU_USAGE': 'sum(node_load15) / sum(openstack_nova_vcpus)',
    'US_NODES_RAM_ALLOC': 'sum(openstack_nova_ram - openstack_nova_free_ram) / sum(openstack_nova_ram)',
    'US_NODES_RAM_USAGE': 'sum(mem_used) / sum(openstack_nova_ram) / 1024 / 1024',
    'US_NODES_CPU_ALLOC': 'sum(openstack_nova_used_vcpus) / sum(openstack_nova_vcpus)',
    'US_NODES_CPU_USAGE': 'sum(system_load15) / sum(openstack_nova_vcpus)',
    'EU_PROJECT_QUOTA_RAM': 'avg(openstack_nova_quota_ram{project_name=~".*team"}) by (project_name)',
}
