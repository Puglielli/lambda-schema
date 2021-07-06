import json
import os
import requests

topics = []
createList = []
updateList = []
deleteList = []
compatibility = []


cur_path = f'{os.getcwd()}/schemas/'
URL = 'http://localhost:8081'
mapFiles = dict()


# Get Metadate
with open('metadata.json') as json_file:
    data = json.load(json_file)
    topics = data['Topics']
    createList = data['Create']
    updateList = data['Update']
    deleteList = data['Delete']
    compatibility = data['Compatibility']


def init():
    print("#### Conta Modernizada ####")
    print("#### Iniciando Lambda dos Schemas ####")
    print(f'Schemas: {str(list_schemas())}')
    get_files()

    for k, v in mapFiles.items():
        if v['event'] == 'C':
            create_schema(k, v['schema'])
        elif  v['event'] == 'U':
            update_schema(k, v['schema'], v['compatibility'])
        elif  v['event'] == 'D':
            delete_schema(k)


def list_schemas():
    request = requests.get(url=f'{URL}/subjects')
    return request.json()


def create_schema(subject, schema):
    request = requests.post(
        url=f'{URL}/subjects/{subject}/versions',
        data=schema,
        headers={'content-type': 'application/vnd.schemaregistry.v1+json'}
    )
    response = request.json()
    print(f'Responde: {response}')


def update_schema(subject, schema, compatibility):
    request = requests.post(
        url=f'{URL}/subjects/{subject}',
        data=schema,
        headers={'content-type': 'application/vnd.schemaregistry.v1+json'}
    )
    response = request.json()
    print(f'Responde: {response}')


def update_compatibility_schema(subject, compatibility):
    request = requests.put(
        url=f'{URL}/config/{subject}',
        data="{'compatibility':" + f'{compatibility}',
        headers={'content-type': 'application/vnd.schemaregistry.v1+json'}
    )
    response = request.json()
    print(f'Responde: {response}')


def delete_schema(subject):
    request = requests.delete(url=f'{URL}/subjects/{subject}')
    response = request.json()
    print(f'Responde: {response}')


def get_key_by_dict_value(value, dictionary):
    key=''
    for k, v in dictionary.items():
        if len(v) > 0 and v[0] == value:
            key=k
    return key


def get_event(schema):
    event=''
    for i in createList:
        if schema == i:
            event="C"
            break
    for i in updateList:
        if schema == i:
            event="U"
            break
    for i in deleteList:
        if schema == i:
            event="D"
            break
    return event


def get_files():
    for file in os.listdir('schemas'):
        schema=json.load(open(f'{cur_path}/{file}'))
        topic=get_key_by_dict_value(file, topics)
        # topic-namespace.name
        subject=f"{topic}-{schema['namespace']}.{schema['name']}"
        mapFiles[subject] = {
            'topic': f'{topic}',
            'schema': "{'schema': " + str(schema) + " }",
            'compatibility': f'{get_key_by_dict_value(file, compatibility)}',
            'event': f'{get_event(file)}'
        }


if __name__ == '__main__':
    init()