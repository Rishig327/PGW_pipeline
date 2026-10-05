from setuptools import setup

setup(
    name="PGW_pipeline",
    version="0.1.0",
    py_modules=[
        "Extract_Compressed_Files",
        "beam_ber_asn_decoder",
        "config_PGW",
        "config_common",
        "transformation_PGW",
        "decode_functions",
        "Lookups_config",
        "LookupsManager",
        "aggregate_funcation"
    ],


)