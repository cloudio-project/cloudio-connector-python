import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from typing import List
from dataclasses import dataclass
from threading import Thread
import pandas as pd
import queue


@dataclass()
class AttributeId:
    friendly_name: str
    node: str
    objects: List[str]
    attribute: str

    def __str__(self):
        return self.friendly_name + '/' + self.node + '/' + '/'.join(self.objects) + '/' + self.attribute


@dataclass()
class TimeSeries:
    attribute_id: AttributeId
    start: datetime
    stop: datetime
    resample: str = None


class CloudioConnector:
    def __init__(self, host, user, password, max_points=10000):
        '''
        Initializer
        :param host: the cloudio host
        :param user: the cloudio user
        :param password: the cloudio password
        :param max_points: the maximum number of points per GET
        '''
        self._user = user
        self._password = password
        self._host = host
        self._max_points = max_points

    def get_uuid(self, friendly_name):
        '''
        Get a UUID from a friendly name
        @param friendly_name: the friendly name
        @return: corresonding UUID
        '''
        params = {'friendlyName': friendly_name}
        url = self._host + "/api/v1/endpoints"
        endpoint = requests.get(url, auth=HTTPBasicAuth(self._user, self._password), params=params).json()
        return endpoint[0]['uuid']

    def get_time_serie(self, time_serie: TimeSeries):
        '''
        Get the historical data of an attribute
        @param time_serie: the attribute and time serie parameters
        @return: the attribute historical data
        '''
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        date_format_2 = "%Y-%m-%dT%H:%M:%SZ"

        url = self._host + "/api/v1/history/"
        uuid = self.get_uuid(time_serie.attribute_id.friendly_name)

        url += uuid + '/' + time_serie.attribute_id.node
        for i in time_serie.attribute_id.objects:
            url += '/' + i
        url += '/' + time_serie.attribute_id.attribute

        finished = False

        params = {"max": self._max_points}

        start = time_serie.start
        stop = time_serie.stop

        total = 0

        if time_serie.resample is not None:
            params["resampleInterval"] = time_serie.resample

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

    def get_mean_value(self, attribute_id, period):
        '''
        Get the mean value of an attribute
        @param attribute_id: the attribute to get value from
        @param period: the mean period
        @return: the mean value
        '''
        start = (datetime.utcnow() - timedelta(seconds=period))
        stop = datetime.utcnow()

        data = self.get_time_serie(TimeSeries(attribute_id, start, stop))

        res = 0
        count = 0

        for dp in data:
            res += dp['value']
            count += 1

        return res / count

    def data_frame(self, data, serie_name='value'):
        '''
        Convert a Cloud.iO time serie data to panda data frame
        @param data: the cloudio data to convert
        @param serie_name: the name of the panda data frame serie
        @return: the panda data frame
        '''
        index = []
        values = []

        for i in data:
            index.append(i['time'])
            values.append(i['value'])

        return pd.DataFrame(data=values, index=pd.to_datetime(index), columns=[serie_name])

    def get_multiple_time_series(self, series: List[TimeSeries], no_workers=5):
        '''
        Get multiple time series in parallel using multi threading
        @param series: the time series to get
        @param no_workers: the number of workers
        @return: the time series
        '''
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
                    response = self.cc.get_time_serie(time_serie=content)
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
