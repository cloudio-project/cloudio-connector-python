# cloudio-connector-python
This library is a helper to create python cloudio applications.

Example:
```
class Example(AttributeListener):
    def __init__(self):
        cc = CloudioConnector("https://example.com", "user", "password")
        cc.add_attribute_listener(self)

        attr = AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='mySetPoint'),
        
        # subscribe to attribute on change event
        cc.subscribe_to_attribute(attr)

        # get attribute time series
        tm = TimeSeries(attr, start=datetime.now() - datetime.timedelta(hours=2), stop=datetime.now(), resample='15m')
        data = cc.get_time_series(tm)

        # get the last value of an attribute
        last_val = cc.get_last_value(attr)

        # write the value of an attribute
        cc.write_value(attr, 1.0)      
        
    # this method is called when a subscribed attribute has changed
    def attribute_has_changed(self, attribute: AttributeId, value):
        print(str(attribute) + ' ' + str(value))
```