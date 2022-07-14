# cloudio-connector-python
This library is a helper to create python cloudio **applications**.

The cloudio project: http://cloudio.hevs.ch/

## The cloud.iO three layers
Cloud.iO is composed of 3 layers:

<img src="https://github.com/cloudio-project/cloudio-connector-python/blob/develop/doc/images/three_layers.PNG" alt="drawing" width="200"/>

- The endpoints: distributed field devices that mainly measures and actuate things.
- Cloudio services: the cloud.iO server that communicate with the endpoints, stores the data and provide a http rest api to the applications.
- **The applications**: data analysis, controls the endpoint setpoints, ...

**This library is used to create applications.**

## The cloud.iO data model
Here is a quick reminder of the cloud.iO data model:
![alt text](https://github.com/cloudio-project/cloudio-connector-python/blob/develop/doc/images/data_model.PNG?raw=true)

**Note: You can have objects in objects, that's why an object list is needed to create an AttributeId.**

## Read/Write attributes
### Example
```
cc = CloudioConnector("https://example.com", "user", "password")

sp = AttributeId(uuid='ba3d3ec2-23b6-45a8-827a-3b3133a69076', node='myNode', objects=['myObject'], attribute='mySetPoint')
mea = AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='myMeasure')

# get the last value of an attribute
last_val = cc.get_last_value(mea)
print(str(mea) + ' ' + str(last_val))

# get the mean value of the last 15 minutes
mean = cc.get_mean_value(mea, 15*60)

# write the value of an attribute
cc.write_value(sp, 1.0)      
```
## Get attributes time series
### Example
```
cc = CloudioConnector("https://example.com", "user", "password")

sp = AttributeId(uuid='ba3d3ec2-23b6-45a8-827a-3b3133a69076', node='myNode', objects=['myObject'], attribute='mySetPoint')
mea = AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='myMeasure')

tm_mea = TimeSeries(mea, start=datetime.now() - datetime.timedelta(hours=24), stop=datetime.now(), resample='15m')
tm_sp = TimeSeries(sp, start=datetime.now() - datetime.timedelta(hours=24), stop=datetime.now(), resample='15m')

# get attribute time series
data = cc.get_time_series(tm_mea)

# convert cloudio time series to panda data frame
cc.data_frame(data, serie_name="My Measure")

# get multiple time series with multithreading
data = cc.get_multiple_time_series([tm_mea, tm_sp])
    
```
## Get notified for attribute new value
### Example
```
class Example(AttributeListener):
    def __init__(self):
        cc = CloudioConnector("https://example.com", "user", "password")
        cc.add_attribute_listener(self)

        attr = AttributeId(uuid=cc.get_uuid('demo'), node='myNode', objects=['myObject'], attribute='myMeasure'),
        
        # subscribe to attribute on change event
        cc.subscribe_to_attribute(attr)
        
    # this method is called when a subscribed attribute has changed
    def attribute_has_changed(self, attribute: AttributeId, value):
        print(str(attribute) + ' ' + str(value))
```
