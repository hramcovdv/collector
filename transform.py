INT_TYPE = (
    'TICKS',
    'INTEGER',
    'COUNTER64'
)

FLOAT_TYPE = (
    'GAUGE'
)

NONE_TYPE = (
    'NOSUCHOBJECT',
    'NOSUCHINSTANCE'
)

def get_snmp_value(snmp_class):
    # Return SNMP value with correct type

    if snmp_class.snmp_type in INT_TYPE:
        return int(snmp_class.value)

    if snmp_class.snmp_type in FLOAT_TYPE:
        return float(snmp_class.value)

    return snmp_class.value

def get_point(snmp_class):
    # Return InfluxDB point

    if snmp_class.snmp_type in NONE_TYPE:
        return None

    point = {
        'measurement': snmp_class.oid,
        'fields': {'value': get_snmp_value(snmp_class)},
        'tags': {}
    }

    if snmp_class.oid_index is not None:
        point['tags']['index'] = snmp_class.oid_index

    return point

def get_points(snmp_vars):
    return list(filter(None, map(get_point, snmp_vars)))
