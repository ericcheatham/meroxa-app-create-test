import logging
import sys

from turbine.runtime import RecordList, Runtime

logging.basicConfig(level=logging.INFO)


def transform(records: RecordList) -> RecordList:
    print(records)
    return


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:

            source = await turbine.resources("uwupostgres")

            records = await source.records("customer_order")

            turbine.register_secrets("PWD")

            # transformed = await turbine.process(records, transform)

            destination_webhook = await turbine.resources("webhook-dest")
            destination_webhook_two = await turbine.resources("webhook-dest-two")

            await destination_webhook.write(records, "collection_archive", {})
            await destination_webhook_two.write(records, "collection_archive", {})

        except Exception as e:
            print(e, file=sys.stderr)
