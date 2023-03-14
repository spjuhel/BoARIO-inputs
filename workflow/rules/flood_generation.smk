rule all_rep_events:
    input:
        real=expand("{outputdir}/builded-data/{mrio_name}/representative_events_{mrio_name}_{period}.parquet",outputdir=config["FLOOD_DATA_DIR"],period=["1970_2015","2016_2035","2036_2050"],mrio_name=["euregio_full","exiobase3_74_sectors"]),
        test=expand("{outputdir}/builded-data/{mrio_name}/test_representative_events_{mrio_name}_{server}.parquet",outputdir=config["FLOOD_DATA_DIR"],mrio_name=["euregio_full","exiobase3_74_sectors"],server=["local","server"])

rule flood_mapping:
    """
    Create floods files
    """
    input:
        folder = config["FLOOD_DATA_DIR"],
        shapes = config["MRIO_SHAPES_FILE"],
        flopros = config["FLOPROS_FILE"],
    output:
        expand("{folder}/builded-data/{{mrio_basename}}{{mrio_subname}}/6_full_floodbase_{{mrio_basename}}{{mrio_subname}}_{period}.parquet",folder=config["FLOOD_DATA_DIR"],period=["1970_2015","2016_2035","2036_2050"])
    wildcard_constraints:
        mrio_basename="euregio|exiobase3|icio|wiod",
        mrio_subname=".{0}|_full|_\d+_sectors"
    resources:
        mem_mb=6000
    threads:
        4
    conda:
        "versa"
    params:
        ref_year=2010,
        builded_data = config["MRIO_DATA_DIR"]
    shell:
        """
        python {config[BOARIO-TOOLS]}/flood-mrio-mapping.py -i {input.folder} -o {input.folder} -s {input.shapes} -m {wildcards.mrio_basename}{wildcards.mrio_subname} -P {input.flopros} -r {params.ref_year} -B {params.builded_data}/{wildcards.mrio_basename}/builded-files/
        """
rule rep_event:
    """
    generate representative events
    """
    input:
        expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/6_full_floodbase_{{mrio_basename}}{{mrio_subname}}_{{period}}.parquet",outputdir=config["FLOOD_DATA_DIR"])
    output:
        expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/representative_events_{{mrio_basename}}{{mrio_subname}}_{{period}}.parquet",outputdir=config["FLOOD_DATA_DIR"])
    wildcard_constraints:
        mrio_basename="euregio|exiobase3|icio|wiod",
        mrio_subname=".{0}|_full|_\d+_sectors"
    conda:
        "versa"
    shell:
        """
        python {config[BOARIO-TOOLS]}/representative_events.py -i {input} -o {output}
        """


rule test_flood_and_rep_event:
    """
    general minimalist flood db and representative events
    """
    input:
        flood = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/6_full_floodbase_{{mrio_basename}}{{mrio_subname}}_2016_2035.parquet",outputdir=config["FLOOD_DATA_DIR"]),
        rep = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/representative_events_{{mrio_basename}}{{mrio_subname}}_2016_2035.parquet",outputdir=config["FLOOD_DATA_DIR"])
    output:
        rep_events = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/test_representative_events_{{mrio_basename}}{{mrio_subname}}_{server}.parquet",outputdir=config["FLOOD_DATA_DIR"],server=["local","server"]),
        flood_db = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/test_floodbase_{{mrio_basename}}{{mrio_subname}}_{server}.parquet",outputdir=config["FLOOD_DATA_DIR"],server=["local","server"])
    params:
        out_1 = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/test_floodbase_{{mrio_basename}}{{mrio_subname}}",outputdir=config["FLOOD_DATA_DIR"]),
        out_2 = expand("{outputdir}/builded-data/{{mrio_basename}}{{mrio_subname}}/test_representative_events_{{mrio_basename}}{{mrio_subname}}",outputdir=config["FLOOD_DATA_DIR"])

    wildcard_constraints:
        mrio_basename="euregio|exiobase3|icio|wiod",
        mrio_subname=".{0}|_full|_\d+_sectors"
    conda:
        "versa"
    shell:
        """
        python {config[BOARIO-TOOLS]}/representative_events_test.py -if {input.flood} -ir {input.rep} -of {params.out_1} -or {params.out_2}
        """
