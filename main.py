import logging
import sys

from turbine.runtime import RecordList, Runtime

logging.basicConfig(level=logging.INFO)


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:

            source = await turbine.resources("pg")

            records = await source.records("customer_order")


            dest = await turbine.resources("datalake")

            await dest.write(records, "whatever",  {"file.name.template": "{{timestamp:unit=yyyy}}/{{timestamp:unit=MM}}/{{timestamp:unit=DD}}/{{timestamp:unit=hh}}/{{timestamp:unit=mm}}/file.gz"})

        except Exception as e:
            print(e, file=sys.stderr)
