rule minimal_exio:
    input:
        MINIMAL_7_EXIO

rule all_full_exio:
    input:
        ALL_FULL_EXIO

rule all_74_sect_exio:
    input:
        ALL_74_EXIO

rule generate_subregions_mrio:
    input:
        in_mrio = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}.pkl",outputdir=config["BUILDED_DATA_DIR"]),
        #mrio_params = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}_params.json",outputdir=config["BUILDED_DATA_DIR"])
    output:
        out_mrio = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}_{{subregions}}.pkl",outputdir=config["BUILDED_DATA_DIR"]),
        #out_params = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}_{{subregions}}_params.json",outputdir=config["BUILDED_DATA_DIR"])
    conda:
        "env/boario-use.yml"
    wildcard_constraints:
        sector_aggreg_name="exiobase3_(?:Full|\d+_sectors)",
        region_aggreg_name="FullWorkd|[A-Z]{2}-RoW",
#        subregions="[A−Z]{2}_sliced_in_\d*"
    resources:
        vmem_mb=6000,
        mem_mb=5000,
        disk_mb=2000
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/mrio_subregions.py {input.in_mrio} {wildcards.subregions} -o {output.out_mrio}
        """

rule generate_mrio_full_from_zip:
    input:
        mrio_file = expand("{inputdir}/IOT_{{year}}_ixi.zip",inputdir=config["SOURCE_DATA_DIR"])
    #params:
    #    full_mrio_params = expand("{outputdir}/mrios/exiobase3_full_params.json",outputdir=config["BUILDED_DATA_DIR"])
    conda:
        "env/boario-use.yml"
    benchmark:
        "mrios/exiobase3_{year}_full_bench.csv"
    output:
        mrioout = expand("{outputdir}/mrios/exiobase3_{{year}}_full.pkl",outputdir=config["BUILDED_DATA_DIR"])
    resources:
        vmem_mb=8000,
        mem_mb=7000,
        disk_mb=2000
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/build_pkl.py -t "EXIO3" -o {output.mrioout} -i {input.mrio_file};
        """

rule mrio_params_build:
    input:
        ods = expand("{folder}/exiobase3_{{sector_aggreg_name}}_params.ods",folder=config["SOURCE_DATA_DIR"])
    params:
        monetary = 1000000,
        main_inv_duration = 90
    resources:
        vmem_mb=1000,
        mem_mb=1000,
        disk_mb=1000
    output:
        mrio_params_json = expand("{outputdir}/mrios/exiobase3_{{sector_aggreg_name}}_params.json",outputdir=config["BUILDED_DATA_DIR"]),
        event_params_json = expand("{outputdir}/mrios/exiobase3_{{sector_aggreg_name}}_event_params.json",outputdir=config["BUILDED_DATA_DIR"])
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/mrio_json_params_build.py {input.ods} {params.monetary} {params.main_inv_duration} -po {output.mrio_params_json} -eo {output.event_params_json}
        """

rule mrio_sector_aggreg:
    input:
        full_mrio_file = rules.generate_mrio_full_from_zip.output.mrioout,
        sector_aggreg_file = expand("{folder}/exiobase3_{{sector_aggreg_name}}.ods",folder=config["AGGREG_FILES_DIR"])
    params:
        full_mrio_params = expand("{folder}/exiobase3_{{sector_aggreg_name}}_params.ods",folder=config["SOURCE_DATA_DIR"])
    conda:
        "env/boario-use.yml"
    output:
        out_mrio = expand("{folder}/mrios/exiobase3_{{year}}_{{sector_aggreg_name}}.pkl",folder=config["BUILDED_DATA_DIR"])
    wildcard_constraints:
        #sector_aggreg_name="(.(?!(full)))*"
        sector_aggreg_name="\d+_sectors",
        year="\d\d\d\d"
    resources:
        vmem_mb=6000,
        mem_mb=5000,
        disk_mb=2000
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/aggreg_exio3_sectors.py {input.full_mrio_file} {input.sector_aggreg_file} -o {output.out_mrio}
        """

rule mrio_one_region_RoW_aggreg:
    input:
        mrio_file = expand("{outputdir}/mrios/{{mrio}}_FullWorld.pkl",outputdir=config["BUILDED_DATA_DIR"]),
        region_aggreg_file = expand("{inputdir}/aggreg/{{region}}_aggreg.json",inputdir=config["BUILDED_DATA_DIR"]),
        old_mrio_params = expand("{outputdir}/mrios/{{mrio}}_FullWorld_params.json",outputdir=config["BUILDED_DATA_DIR"])
    conda:
         "env/boario-use.yml"
    output:
        out_mrio = expand("{outputdir}/mrios/{{mrio}}_{{region}}-RoW.pkl",outputdir=config["BUILDED_DATA_DIR"]),
        out_params = expand("{outputdir}/mrios/{{mrio}}_{{region}}-RoW_params.json",outputdir=config["BUILDED_DATA_DIR"])
    resources:
         vmem_mb=6000,
         mem_mb=5000,
         disk_mb=2000
    wildcard_constraints:
        region="[A-Z]{2}"
    shell:
         """
         nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/aggreg_exio3_region.py {input.mrio_file} {input.region_aggreg_file} {input.old_mrio_params} -o {output.out_mrio} -po {output.out_params}
         """

rule region_aggreg_dict:
    output:
        expand("{inputdir}/aggreg/{{region}}_aggreg.json",inputdir=config["BUILDED_DATA_DIR"])
    run:
        import json
        dic = {"aggregates":{str(wildcards.region):str(wildcards.region)},
            "missing":"RoW"}
        with open(output[0],'w') as f:
            json.dump(dic, f)
