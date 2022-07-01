import requests
from requests.auth import HTTPBasicAuth
from requests.models import PreparedRequest
from datetime import datetime, timedelta
import time
from typing import List
from dataclasses import dataclass
from threading import Thread
import pandas as pd
import queue
import sseclient
import json

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
        self._sse_client: sseclient.SSEClient
        self._observed_attributes: List[AttributeId] = list()
        self._attribute_listeners: List[AttributeListener] = list()
        self._observed_attributes_updated = False

        Thread(target=self._get_events).start()

    def get_uuid(self, friendly_name):
        """
        Get a UUID from a friendly name
        :param friendly_name: the friendly name
        :return: corresponding UUID
        """
        params = {'friendlyName': friendly_name}
        url = self._host + "/api/v1/endpoints"
        endpoint = requests.get(url, auth=HTTPBasicAuth(self._user, self._password), params=params).json()
        return endpoint[0]['uuid']

    def get_friendly_name(self, uuid):
        """
        Get a friendly name from a uuid
        :param uuid: the uuid
        :return: the corresponding friendly name
        """
        url = self._host + "/api/v1/endpoints/" + uuid
        endpoint = requests.get(url, auth=HTTPBasicAuth(self._user, self._password)).json()
        return endpoint['friendlyName']

    def get_time_series(self, time_series: TimeSeries):
        """
        Get the historical data of an attribute
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

        # request 10000 datapoint per loop
        while not finished:
            params['from'] = start.strftime(date_format)

            total += self._max_points

            data = requests.get(url, auth=HTTPBasicAuth(self._user, self._password), params=params).json()

            for i in data:
                # get the datapoint time
                try:
                    time = datetime.strptime(i['time'], date_format)
                except ValueError:
                    time = datetime.strptime(i['time'], date_format_2)

                # check if stop time is reached
                if time < stop:
                    result.append(i)
                else:
                    finished = True

            # get the last datapoint time
            try:
                start = datetime.strptime(data[-1]['time'], date_format)
            except ValueError:
                start = datetime.strptime(data[-1]['time'], date_format_2)

            # add a second to the next start time
            start = start + timedelta(seconds=1)

            # exit if list is empty
            if len(data) < self._max_points:
                finished = True

        return result

    def get_last_value(self, attribute_id):
        """
        Get the last value of an attribute
        :param attribute_id: the attribute to get value from
        :return: the last value
        """
        url = self._host + "/api/v1/data/" + str(attribute_id)

        data = requests.get(url, auth=HTTPBasicAuth(self._user, self._password)).json()

        return data['value']

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

        requests.put(url, auth=HTTPBasicAuth(self._user, self._password))

    def data_frame(self, data, serie_name='value'):
        """
        Convert a Cloud.iO time series data to panda data frame
        :param data: the cloudio data to convert
        :param serie_name: the name of the panda data frame serie
        :return: the panda data frame
        """
        index = []
        values = []

        for i in data:
            index.append(i['time'])
            values.append(i['value'])

        return pd.DataFrame(data=values, index=pd.to_datetime(index), columns=[serie_name])

    def get_multiple_time_series(self, series: List[TimeSeries], no_workers=5):
        """
        Get multiple time series in parallel using multi threading
        :param series: the time series to get
        :param no_workers: the number of workers
        :return: the time series
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
                    serie_id = str(content.attribute_id)
                    response = self.cc.get_time_series(time_series=content)
                    self.results[serie_id] = response
                    self.queue.task_done()

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

        # Combine results from all workers
        r = {}
        for worker in workers:
            r = {**r, **worker.results}
        return r

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
        subscribe to an attribute changes
        :param attribute: the attribute
        """
        self._observed_attributes.append(attribute)
        self._observed_attributes_updated = True

    def unsubsribe_from_attribute(self, attribute):
        """
        unsubscribe from an attribute changes
        :param attribute: the attribute
        """
        self._observed_attributes.remove(attribute)
        self._observed_attributes_updated = True

    def _get_events(self):
        """
        method used internally to catch attributes changes
        """
        while True:
            while len(self._observed_attributes) == 0:
                time.sleep(0.1)  # let other threads work

            self._observed_attributes_updated = False

            while not self._observed_attributes_updated:
                url = self._host + '/api/v1/data/subscribe'
                headers = {'Content-type': 'application/json'}
                body = json.dumps(list(map(str, self._observed_attributes)))
                response = requests.post(url=url, headers=headers, data=body, stream=True,
                                         auth=HTTPBasicAuth(self._user, self._password))

                if response.status_code != 200:
                    time.sleep(10.0)  # to be sure to not spam the server

                self._sse_client = sseclient.SSEClient(response)

                for event in self._sse_client.events():
                    data = json.loads(event.data)
                    for a in self._observed_attributes:
                        if str(a) == data['id']:
                            for listener in self._attribute_listeners:
                                listener.attribute_has_changed(a, data['value']['value'])
                    if self._observed_attributes_updated:
                        break
