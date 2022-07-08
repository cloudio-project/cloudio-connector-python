import time
from datetime import datetime
from threading import Thread

from src.cloudio_connector import AttributeListener, CloudioConnector, AttributeId


class Tester(AttributeListener):
    def __init__(self):
        cc = CloudioConnector("https://example.com", "user", "password")
        cc.add_attribute_listener(self)

        # while True:
        attr = [
            AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='myMeasure'),
            AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='myInt')]
        while True:
            cc.subscribe_to_attribute(attr[0])
            time.sleep(10)
            # cc.subscribe_to_attributes([])
            # time.sleep(10)


    def attribute_has_changed(self, attribute: AttributeId, value):
        print(str(attribute) + ' ' + str(value))


if __name__ == '__main__':
    for i in range(0, 1):
        Thread(target=Tester).start()
        time.sleep(0.1)
    while True:
        time.sleep(0.01)
