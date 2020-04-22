import pyodbc
import pandas
import requests
import ldap
import numpy
from datetime import date

host = 'localhost'
port_rest = '9047'
ldap_host = 'ldap://localhost:389'
port = '31010'
uid = 'user'
pwd = 'pwd'
driver = 'Dremio Connector'

cnxn = pyodbc.connect("Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={}".format(
    driver, host, port, uid, pwd), autocommit=True)
sql = '''select * FROM "@jonny".DremioVDS'''
df = pandas.read_sql(sql, cnxn)

BASE_URL = 'http://' + host + ':' + port_rest
headers = {'Content-Type': 'application/json'}
data = '{"userName": "' + uid + '","password": "' + pwd + '"}'
response = requests.post(BASE_URL + '/apiv2/login',
                         headers=headers, data=data, verify=False)
authorization_code = '_dremio' + response.json()['token']

auth_header = {
    'Authorization': authorization_code,
    'Content-Type': 'application/json'}

# LDAP Setup
con = ldap.initialize(ldap_host, bytes_mode=False)
con.simple_bind_s(u'CN=admin,DC=dremio,DC=com', u'dremio123')

# Groups to lookup in LDAP
groups = []
rows_list = []

sor = []
for ds in df['vdss']:
    j = ''
    while '/' in ds:
        if len(j) != 0:
            j = j + '/' + ds.partition('/')[0]
        else:
            j = ds.partition('/')[0]
        sor.append(j)
        ds = ds.partition('/')[2]
setsor = set(sor)
df = pandas.concat([df['vdss'], pandas.DataFrame(list(setsor))])
df = df.rename(columns={0: "vdss"})
t = 0
for i in df['vdss']:
    row = {}
    response = requests.request(
        'GET', BASE_URL + '/api/v3/catalog/by-path/' + i, headers=auth_header)
    datasetname = i.replace('%20', ' ').replace('/', '.')
    if response.status_code != 400:
        try:
            objecttype = response.json()['type']
        except:
            try:
                objecttype = response.json()['entityType']
            except:
                ''
        if objecttype == 'VIRTUAL_DATASET':
            sql = response.json()['sql'].replace('\n', ' ')
        else:
            sql = ''
        try:
            acls = response.json()['accessControlList']
            for acl_lists in acls.values():
                if isinstance(acl_lists, list):
                    for acl in acl_lists:
                        if isinstance(acl, dict):
                            for vals in acl.values():
                                readaccess = 'False'
                                writeaccess = 'False'
                                if isinstance(vals, str):
                                    row['UpdatedDate'] = date.today()
                                    row['Object'] = datasetname
                                    row['ObjectType'] = objecttype
                                    row['ObjectDefinition'] = sql
                                    if not con.search_s('OU=users,O=dremio,DC=dremio,DC=com', ldap.SCOPE_SUBTREE, 'CN=' + vals):
                                        row['Username'] = ''
                                        row['AuthorizedBy'] = 'GROUP'
                                        row['GroupName'] = vals
                                        if vals not in groups:
                                            groups.append(vals)
                                    else:
                                        row['Username'] = vals
                                        row['AuthorizedBy'] = 'USERNAME'
                                        row['GroupName'] = ''
                                else:
                                    for permission in vals:
                                        if (permission == 'READ'):
                                            readaccess = 'True'
                                        elif (permission == 'WRITE'):
                                            writeaccess = 'True'
                                row['ReadAccess'] = readaccess
                                row['WriteAccess'] = writeaccess
                                row['Inheritence'] = ''
                                if row not in rows_list:
                                    rows_list.append(row)
                elif isinstance(acl_lists, str):
                    row['UpdatedDate'] = date.today()
                    row['Object'] = datasetname
                    row['ObjectType'] = objecttype
                    row['ObjectDefinition'] = sql
                    row['Username'] = ''
                    if '/' in datasetname:
                        row['AuthorizedBy'] = 'INHERITED'
                        row['GroupName'] = ''
                        row['ReadAccess'] = None
                        row['WriteAccess'] = None
                        row['Inheritence'] = datasetname.partition('/')[0]
                    else:
                        row['AuthorizedBy'] = 'EVERYONE'
                        row['GroupName'] = 'EVERYONE'
                        row['ReadAccess'] = 'True'
                        row['WriteAccess'] = 'True'
                        row['Inheritence'] = ''
                    if 'EVERYONE' not in groups:
                        groups.append('EVERYONE')
                    if row not in rows_list:
                        rows_list.append(row)
        except:
            row['UpdatedDate'] = date.today()
            row['Object'] = datasetname
            row['ObjectType'] = objecttype
            row['ObjectDefinition'] = sql
            row['Username'] = ''
            if '/' in datasetname:
                row['AuthorizedBy'] = 'INHERITED'
                row['GroupName'] = ''
                row['ReadAccess'] = None
                row['WriteAccess'] = None
                row['Inheritence'] = datasetname.partition('/')[0]
            else:
                row['AuthorizedBy'] = 'EVERYONE'
                row['GroupName'] = 'EVERYONE'
                row['ReadAccess'] = 'True'
                row['WriteAccess'] = 'True'
                row['Inheritence'] = ''
            if 'EVERYONE' not in groups:
                groups.append('EVERYONE')
            if row not in rows_list:
                rows_list.append(row)
userpermissions = pandas.DataFrame(rows_list)

# print(userpermissions)
grpmems = []
for group in groups:
    if group == 'EVERYONE':
        userlist = con.search_s(
            'OU=users,O=dremio,DC=dremio,DC=com', ldap.SCOPE_SUBTREE, 'CN=*')
    else:
        userlist = con.search_s('OU=users,O=dremio,DC=dremio,DC=com', ldap.SCOPE_SUBTREE,
                                'memberOf=CN=' + group + ',OU=groups,O=dremio,DC=dremio,DC=com')
    for user in userlist:
        grpmem = {}
        grpmem['Username'] = user[1]['cn'][0].decode("utf-8")
        grpmem['GroupName'] = group
        if grpmem not in grpmems:
            grpmems.append(grpmem)
grps = pandas.DataFrame(grpmems)
monitoring = pandas.merge(
    userpermissions, grps, left_on='GroupName', right_on='GroupName', how='left')
monitoring = monitoring.replace(numpy.nan, '', regex=True)
monitoring['Username'] = monitoring['Username_x'] + monitoring['Username_y']
monitoring = monitoring.drop(['Username_x', 'Username_y'], axis=1)
monitoring.to_parquet('/Users/jonny/Google Drive (jonny@dremio.com)/Permissions/Permissions log/monitoring' +
                      str(date.today()) + '.parquet', compression='snappy')
