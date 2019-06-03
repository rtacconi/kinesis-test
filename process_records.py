'''
Process records sent by the Kinesis consumer
'''


def do(records):
    for r in records:
        print(r)

    return True
