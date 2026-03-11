

from rx import create
from rx import operators as ops


def kafka_observable(consumer):
    def _observable(observer, _):
        try:
            for msg in consumer:
                observer.on_next(msg.value)
        except Exception as e:
            observer.on_error(e)
    return create(_observable)

def machine_codes():
    return {"UNS56A": "UNSCRAMBLER",
        "WS964F": "WASHER",
        "IS8710": "INSPECTION",
        "FB713A": "FILLING",
        "C7841R": "CARBONATOR",
        "CPM784": "CAPPING",
        "LBL74F": "LABELING",
        "PLL741": "PALLETIZER"
    }

def machines_mapping(code):
    return machine_codes()[code]

def properties_mapping(code):
   return {
       "A7": "LITERS",
       "W8": "QUALITY",
       "L1": "LIGHT",
       "T3": "TIME",
       "P6": "POWER",
       "G8": "GRADES"
   }[code]


def attributes_mapping(code):
   return {
       "TS": "TIMESTAMP",
       "MC": "MACHINE",
       "PR": "PRODUCT",
       "PS": "PROPS"
   }[code]

def auxa(event):
    print(f"a: {event}")

def auxb(event):
    print(f"b: {event}")

def aux(e):
    for k,v in e["PROPS"].items():
        e["PS"][properties_mapping(k)]=value
        del e["PS"][k]
def build_pipeline(source, send_rich_event, save_raw_event, save_rich_event):
    return source.pipe(
        ops.do_action(save_raw_event),
        ops.do_action(lambda _: print("raw event saved")),
        ops.do_action(lambda e: print(f"event received: {e}")),
        ops.map(lambda e: {attributes_mapping(k):e[k] for k in e.keys()}),
        ops.filter(lambda e: e["MACHINE"] in machine_codes().keys()),
        ops.map(lambda e:
               {**e, "MACHINE": machines_mapping(e["MACHINE"])}),
        ops.map(lambda e: {**e, "PROPS": {properties_mapping(k):v for k,v in e["PROPS"].items()}}),
        ops.do_action(send_rich_event),
        ops.do_action(lambda _: print("rich event send")),
        ops.do_action(save_rich_event)

    )


    #TODO: iterar sobre lo