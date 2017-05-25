#!/usr/bin/env python

import argparse
import requests
import json
import os
import time

url = 'https://slack.com/api/'
messages_limit = 1000
files_limit = 1000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', help='token', required=True)
    parser.add_argument('--channel', help='channel', required=True)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('--destination', help='dump destination')
    group.add_argument('--delete', help='delete data for given user', metavar='USER')
    args = parser.parse_args()

    token = args.token
    channel = args.channel
    destination = args.destination
    delete_user = args.delete

    if destination:
        if os.path.exists(destination):
            raise Exception("Path {} already exists, cannot dump data".format(destination))
        os.makedirs(destination)
        os.makedirs(os.path.join(destination, 'files'))

    if not delete_user:
        dump_channel_info(token, channel, destination)

    dump_files(token, channel, destination, delete_user)
    users = dump_messages(token, channel, destination, delete_user)

    if not delete_user:
        dump_users(token, users, destination)


def dump_channel_info(token, channel, destination):
    method_prefix = 'groups' if channel[0] == 'G' else 'channels'
    response = requests.get(url + method_prefix + '.info', params={
        'token': token,
        'channel': channel,
    })

    if response.status_code != 200:
        raise Exception("Cannot get channel info: got bad status code: {}"
                        .format(response.status_code))

    parsed = response.json()

    if destination:
        path = os.path.join(destination, 'channel.json')
        print("Saving {}".format(path))
        with open(path, 'w') as outfile:
            json.dump(parsed, outfile, indent=4, sort_keys=True)
    else:
        print(json.dumps(parsed, indent=4, sort_keys=True))


def dump_messages(token, channel, destination, delete_user):
    method_prefix = 'groups' if channel[0] == 'G' else 'channels'
    latest = ''
    users = set()
    messages = []
    messages_count = 0
    page = 1
    while (True):
        response = requests.get(url + method_prefix + '.history', params={
            'token': token,
            'channel': channel,
            'latest': latest,
            'count': messages_limit
        })

        if response.status_code != 200:
            raise Exception("Cannot get messages: got bad status code: {}"
                            .format(response.status_code))

        parsed = response.json()

        for message in parsed['messages']:
            users.add(message['user'])

        if destination:
            path = os.path.join(destination, 'messages_{}.json'.format(page))
            print("Saving {}".format(path))
            with open(path, 'w') as outfile:
                json.dump(parsed, outfile, indent=4, sort_keys=True)
        else:
            if not delete_user:
                print(json.dumps(parsed, indent=4, sort_keys=True))

        messages_count += len(parsed['messages'])

        if delete_user:
            for message in parsed['messages']:
                if message['user'] == delete_user:
                    messages.append(message['ts'])

        if not parsed['has_more']:
            break
        latest = parsed['messages'][-1]['ts']
        page += 1

    print('Fetched {} messages'.format(messages_count))

    if delete_user:
        for message in messages:
            print('Deleting message {}'.format(message))
            response = requests.post(url + 'chat.delete', params={
                'token': token,
                'channel': channel,
                'ts': message
            })

            if response.status_code != 200:
                raise Exception("Cannot delete message: got bad status code: {}"
                                .format(response.status_code))
            time.sleep(1)

    return users


def dump_files(token, channel, destination, delete_user):
    page = 1
    files = []
    files_count = 0
    while (True):
        response = requests.get(url + 'files.list', params={
            'token': token,
            'channel': channel,
            'page': page,
            'count': files_limit
        })

        if response.status_code != 200:
            raise Exception("Cannot get files: got bad status code: {}"
                            .format(response.status_code))

        parsed = response.json()

        if destination:
            path = os.path.join(destination, 'files_{}.json'.format(page))
            print("Saving {}".format(path))
            with open(path, 'w') as outfile:
                json.dump(parsed, outfile, indent=4, sort_keys=True)
            for file in parsed['files']:
                response = requests.get(file['url_private_download'], headers={
                    'Authorization': 'Bearer {}'.format(token)
                })
                if response.status_code != 200:
                    raise Exception("Cannot download file: got bad status code: {}"
                                    .format(response.status_code))
                path = os.path.join(destination, 'files', file['id'])
                print("Saving {}".format(path))
                with open(path, 'w') as outfile:
                    outfile.write(response.content)
        else:
            if not delete_user:
                print(json.dumps(parsed, indent=4, sort_keys=True))

        files_count += len(parsed['files'])

        if delete_user:
            for file in parsed['files']:
                if file['user'] == delete_user:
                    files.append(file['id'])

        if page >= parsed['paging']['pages']:
            break

        page += 1

    print('Fetched {} files'.format(files_count))

    if delete_user:
        for file in files:
            print('Deleting file {}'.format(file))
            response = requests.post(url + 'files.delete', params={
                'token': token,
                'file': file
            })

            if response.status_code != 200:
                raise Exception("Cannot delete file: got bad status code: {}"
                                .format(response.status_code))
            time.sleep(1)


def dump_users(token, users, destination):
    for user in users:
        response = requests.get(url + 'users.info', params={
            'token': token,
            'user': user
        })

        if response.status_code != 200:
            raise Exception("Cannot get user: got bad status code: {}".format(response.status_code))

        parsed = response.json()

        if destination:
            path = os.path.join(destination, 'user_{}.json'.format(user))
            print("Saving {}".format(path))
            with open(path, 'w') as outfile:
                json.dump(parsed, outfile, indent=4, sort_keys=True)
        else:
            print(json.dumps(parsed, indent=4, sort_keys=True))

    print('Fetched {} users'.format(len(users)))


if __name__ == "__main__":
    main()
