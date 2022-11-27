from com.blyxa.stap import RecordGenerator
from com.blyxa.stap import AvroRecordAsJson
from string import Template
import time

class RecordGeneratorImpl(RecordGenerator):
    def generate(self):
        epoch_time = int(time.time())
        t = Template('{ "name" : "myname", "year":{"int": ${epoch_time}}, "color":null }')
        j = t.substitute(epoch_time=epoch_time)
        r = AvroRecordAsJson('{}'.format(epoch_time), j)
        return r