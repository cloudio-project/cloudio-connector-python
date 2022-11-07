import requests
from requests.auth import HTTPBasicAuth
from requests.models import PreparedRequest
from datetime import datetime, timedelta
from cbor import cbor
from typing import List
from dataclasses import dataclass
from threading import Thread
import pandas as pd
import queue
from paho.mqtt import client as mqtt_client
import json
import collections

from abc import ABCMeta, abstractmethod


@dataclass()
class AttributeId:
    uuid: str
    node: str
    objects: List[str]
    attribute: str

    def __str__(self):
        return self.uuid + '/' + self.node + '/' + '/'.join(self.objects) + '/' + self.attribute


@dataclass()
class TimeSeries:
    attribute_id: AttributeId
    start: datetime
    stop: datetime
    resample: str = None
    data = None


class AttributeListener(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def attribute_has_changed(self, attribute: AttributeId, value):
        """
        Method called when an subscribed attribute has changed
        :param attribute: the AttributeId
        :param value: the new value
        :return: None
        """
        pass


class CloudioConnector:
    def __init__(self, host, user, password, max_points=10000):
        """
        Initializer
        :param host: the cloudio host
        :param user: the cloudio user
        :param password: the cloudio password
        :param max_points: the maximum number of points per GET
        """
        self._user = user
        self._password = password
        self._host = host
        self._max_points = max_points
        self._observed_attributes: List[AttributeId] = list()
        self._attribute_listeners: List[AttributeListener] = list()
        self._observed_attributes_updated = False
        self._mqtt_client = mqtt_client.Client()
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_connect_thread = Thread(target=self._mqtt_connect)
        self._subscribed_attributes = list()
        self._unsubscribed_attributes = list()
        self._endpoint_data = collections.defaultdict(dict)
        self._session = requests.Session()

    def get_uuid(self, friendly_name):
        """
        Get a UUID from a friendly name
        :param friendly_name: the friendly name
        :return: corresponding UUID
        """
        for i in self._endpoint_data.keys():
            if 'friendlyName' in self._endpoint_data[i]:
                if self._endpoint_data[i]['friendlyName'] == friendly_name:
                    return i
        params = {'friendlyName': friendly_name}
        url = self._host + "/api/v1/endpoints"
        endpoint = self._get(url, auth=HTTPBasicAuth(self._user, self._password), params=params).json()
        self._endpoint_data[endpoint[0]['uuid']]['friendlyName'] = friendly_name
        return endpoint[0]['uuid']

    def get_endpoint_structure(self, uuid):
        """
        Get the structure of an endpoint
        :param uuid: the endpoint to get structure from
        :return: the endpoint structure
        """
        if uuid in self._endpoint_data.keys():
            if 'structure' in self._endpoint_data[uuid]:
                return self._endpoint_data[uuid]['structure']
        url = self._host + "/api/v1/data/" + str(uuid)
        data = self._get(url, auth=HTTPBasicAuth(self._user, self._password)).json()
        self._endpoint_data[uuid]['structure'] = data
        return data

    def get_friendly_name(self, uuid):
        """
        Get a friendly name from a uuid
        :param uuid: the uuid
        :return: the corresponding friendly name
        """
        if uuid in self._endpoint_data.keys():
            if 'friendlyName' in self._endpoint_data[uuid]:
                return self._endpoint_data[uuid]['friendlyName']
        url = self._host + "/api/v1/endpoints/" + uuid
        endpoint = self._get(url, auth=HTTPBasicAuth(self._user, self._password)).json()
        return endpoint['friendlyName']

    def get_time_series(self, time_series: TimeSeries):
        """
        Get the historical data of an attribute and return/write the result in the data attribute
        in TimeSeries objects
        :param time_series: the attribute and time series parameters
        :return: the attribute historical data
        """
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        date_format_2 = "%Y-%m-%dT%H:%M:%SZ"

        url = self._host + "/api/v1/history/" + str(time_series.attribute_id)

        finished = False

        params = {"max": self._max_points}

        start = time_series.start
        stop = time_series.stop

        total = 0

        if time_series.resample is not None:
            params["resampleInterval"] = time_series.resample

        result = list()

        while not finished:
            params['from'] = start.strftime(date_format)
            params['to'] = stop.strftime(date_format)
            params['resampleFunction'] = 'MEAN'

            total += self._max_points

            data = self._get(url, auth=HTTPBasicAuth(self._user, self._password), params=params).json()

            # get the last datapoint time
            try:
                last = datetime.strptime(data[-1]['time'], date_format)
            except ValueError:
                last = datetime.strptime(data[-1]['time'], date_format_2)

            # add a second to the next start time
            start = last + timedelta(seconds=1)

            result.extend(data)

            # exit if list count is lower than max points
            if len(data) < self._max_points:
                finished = True

        time_series.data = result
        return result

    def get_last_value(self, attribute_id):
        """
        Get the last value of an attribute
        :param attribute_id: the attribute to get value from
        :return: the last value
        """
        url = self._host + "/api/v1/data/" + str(attribute_id)

        data = self._get(url, auth=HTTPBasicAuth(self._user, self._password)).json()

        return data

    def get_mean_value(self, attribute_id, period):
        """
        Get the mean value of an attribute
        :param attribute_id: the attribute to get value from
        :param period: the mean period
        :return: the mean value
        """
        start = (datetime.utcnow() - timedelta(seconds=period))
        stop = datetime.utcnow()

        data = self.get_time_series(TimeSeries(attribute_id, start, stop))

        res = 0
        count = 0

        for dp in data:
            res += dp['value']
            count += 1

        return res / count

    def write_value(self, attribute_id, value):
        """
        Write an attribute
        :param attribute_id: the attribute to write
        :param value: the value to write
        :return: None
        """
        url = self._host + "/api/v1/data/" + str(attribute_id)

        param = {'value': value}

        req = PreparedRequest()
        req.prepare_url(url, param)
        url = req.url

        self._put(url, auth=HTTPBasicAuth(self._user, self._password))

    def data_frame(self, data):
        """
        Convert a Cloud.iO time series data to panda data frame
        :param data: the cloudio data to convert
        :return: the panda data frame
        """
        return pd.DataFrame(data=data).set_index('time')

    def get_multiple_time_series(self, series: List[TimeSeries], no_workers=None):
        """
        Get multiple time series in parallel using multi threading and write the result in the data attribute
        in TimeSeries objects
        :param series: the time series to get
        :param no_workers: the number of workers
        """

        class Worker(Thread):
            def __init__(self, serie_queue, cloudio_connector: CloudioConnector):
                Thread.__init__(self)
                self.queue = serie_queue
                self.results = {}
                self.cc = cloudio_connector

            def run(self):
                while True:
                    content = self.queue.get()
                    if content == "":
                        break
                    self.cc.get_time_series(time_series=content)
                    self.queue.task_done()

        if no_workers is None:
            no_workers = len(series)

        # Create queue and add series
        q = queue.Queue()
        for serie in series:
            q.put(serie)

        # Workers keep working till they receive an empty string
        for _ in range(no_workers):
            q.put("")

        # Create workers and add tot the queue
        workers = []
        for _ in range(no_workers):
            worker = Worker(q, self)
            worker.start()
            workers.append(worker)
        # Join workers to wait till they finished
        for worker in workers:
            worker.join()

    def add_attribute_listener(self, listener: AttributeListener):
        """
        Add an attribute listener
        :param listener: the listener
        """
        self._attribute_listeners.append(listener)

    def remove_attribute_listener(self, listener: AttributeListener):
        """
        remove an attribute listener
        :param listener: the listener
        """
        self._attribute_listeners.remove(listener)

    def subscribe_to_attribute(self, attribute):
        """
        subscribe to an attribute
        :param attribute: the attribute
        """
        if not self._mqtt_connect_thread.is_alive():
            self._mqtt_connect_thread.start()
        self._subscribed_attributes.append(attribute)
        if self._mqtt_client.is_connected():
            self._mqtt_client.subscribe(topic=self._attr_to_topic(attribute))

    def unsubscribe_from_attribute(self, attribute):
        """
        unsubscribe from an attribute
        :param attribute: the attribute
        """
        self._unsubscribed_attributes.remove(attribute)
        if self._mqtt_client.is_connected():
            self._mqtt_client.unsubscribe(topic=self._attr_to_topic(attribute))

    def _mqtt_connect(self):
        ca_cert = self._get_ca_cert()
        f = open("ca-cert.crt", "w")
        f.write(str(ca_cert))
        f.close()

        self._mqtt_client.tls_set(ca_certs="ca-cert.crt")
        self._mqtt_client.tls_insecure_set(True)
        self._mqtt_client.username_pw_set(self._user, self._password)

        self._mqtt_client.reconnect_delay_set(15, 15)
        self._mqtt_client.connect(host=(self._host.split('://').pop(-1)).split(':').pop(0), port=8883)
        self._mqtt_client.loop_forever()

    def _validate_json(self, data):
        try:
            json.loads(data)
        except ValueError as err:
            return False
        return True

    def _on_message(self, client, userdata, msg):
        if self._validate_json(msg.payload):
            data = json.loads(msg.payload)
        else:
            data = cbor.loads(msg.payload)
        for i in self._attribute_listeners:
            i.attribute_has_changed(self._topic_to_attribute(msg.topic), data)

    def _on_connect(self, client, userdata, flags, rc):
        for i in self._subscribed_attributes:
            self._mqtt_client.subscribe(topic=self._attr_to_topic(i))
        for i in self._unsubscribed_attributes:
            self._mqtt_client.unsubscribe(topic=self._attr_to_topic(i))

    def _topic_to_attribute(self, topic: str):
        if '@update' in topic:
            topic = topic.split('@update/').pop(-1)
        if '@set' in topic:
            topic = topic.split('@set/').pop(-1)
        if self._get_topic_version(topic.split('/').pop(0)) != "v0.2":
            topic = topic.replace('nodes/', '')
            topic = topic.replace('objects/', '')
            topic = topic.replace('attributes/', '')
        return self.str_to_attribute(topic)

    def _get_topic_version(self, uuid):
        return self.get_endpoint_structure(uuid)['version']

    def _get_attr_action(self, attribute: AttributeId):
        data = self.get_endpoint_structure(attribute.uuid)
        node = data['nodes'][attribute.node]
        obj = node
        for i in range(0, len(attribute.objects)):
            obj = obj['objects'][attribute.objects[i]]

        attr = obj['attributes'][attribute.attribute]

        if attr['constraint'] == 'SetPoint' or attr['constraint'] == 'Parameter':
            return 'set'
        else:
            return 'update'

    def _attr_to_topic(self, attribute: AttributeId):

        if self._get_topic_version(attribute.uuid) == "v0.2":
            return '@' + self._get_attr_action(attribute) + '/' + str(attribute)
        else:
            return '@' + self._get_attr_action(
                attribute) + '/' + attribute.uuid + '/nodes/' + attribute.node + \
                   '/objects'.join(attribute.objects) + '/attributes/' + attribute.attribute

    def _get_ca_cert(self):
        url = self._host + "/api/v1/ca-certificate"

        data = self._get(url, auth=HTTPBasicAuth(self._user, self._password))

        return data.text

    def str_to_attribute(self, string: str):
        """
        Converts a str to AttributeId
        :param string: the string to convert
        :return: the attribute id
        """
        sep = string.split('/')
        uuid = sep.pop(0)
        node = sep.pop(0)
        attr = sep.pop(-1)
        return AttributeId(uuid, node, sep, attr)

    def _get(self, url, auth=None, params=None):
        r = self._session.get(url=url, auth=auth, params=params)
        r.raise_for_status()
        return r

    def _put(self, url, auth=None, params=None):
        r = self._session.put(url=url, auth=auth, params=params)
        r.raise_for_status()
        return r
