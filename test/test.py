import time

from src.cloudio_connector import AttributeListener, CloudioConnector, AttributeId


class Tester(AttributeListener):
    def __init__(self):
        cc = CloudioConnector("https://example.com", "user", "password")
        attr = AttributeId(uuid=cc.get_uuid('myEndpoint'), node='node', objects=['object'], attribute='attr')
        cc.subscribe_to_attribute(attr)
        attr = AttributeId(uuid=cc.get_uuid('myEndpoint'), node='node', objects=['object'], attribute='attr2')
        cc.subscribe_to_attribute(attr)
        cc.add_attribute_listener(self)
        time.sleep(10)
        cc.unsubsribe_from_attribute(attr)
        time.sleep(10)
        cc.remove_attribute_listener(self)
        print('end')

    def attribute_has_changed(self, attribute: AttributeId, value):
        print(str(attribute) + " " + str(value))


if __name__ == '__main__':
    t = Tester()
    while True:
        time.sleep(0.01)
