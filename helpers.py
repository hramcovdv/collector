INT_TYPE = (
    "INTEGER",
    "COUNTER",
    "COUNTER64"
    "GAUGE",
    "GAUGE64",
    "TICKS",
    )

STR_TYPE = (
    "STRING",
    "IPADDR",
    )

HEX_TYPE = (
    'OCTETSTR',
    )

NONE_TYPE = (
    "NOSUCHOBJECT",
    "NOSUCHINSTANCE",
    )


def hex_to_str(value, delimiter=' '):
    """ Conver HEX-string to normal string
    """
    return delimiter.join("{:02x}".format(ord(x)) for x in value).upper()


def normalize_value(snmp_variable):
    """ Normalize SNMPVariable value to the correct type
    """
    # Normalize to integer type
    if snmp_variable.snmp_type in INT_TYPE:
        snmp_variable.value = int(snmp_variable.value)

    # Normalize to string type
    elif snmp_variable.snmp_type in STR_TYPE:
        snmp_variable.value = str(snmp_variable.value)

    # Normalize hex-string to normal read string type
    elif snmp_variable.snmp_type in HEX_TYPE:
        snmp_variable.value = hex_to_str(snmp_variable.value)

    # Normalize nosuch to none type
    elif snmp_variable.snmp_type in NONE_TYPE:
        snmp_variable.value = None

    else:
        raise ValueError(f"Undefined SNMPVariable type: {snmp_variable.snmp_type}")

    return snmp_variable


def normal_variable(func):
    """ Normalize SNMPVariable decorator
    """
    def normalization(snmp_variable):
        func(normalize_value(snmp_variable))

    return normalization


@normal_variable
def print_snmp_variable(snmp_variable):
    """ Print SNMPVariable in Net-SNMP format
    """
    print("{oid}.{oid_index} = {snmp_type}: {value}".format(
        oid=snmp_variable.oid,
        oid_index=snmp_variable.oid_index,
        snmp_type=snmp_variable.snmp_type,
        value=snmp_variable.value
    ))
