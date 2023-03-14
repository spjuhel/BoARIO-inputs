ALL_YEARS_EXIO = glob_wildcards(config['MRIO_DATA_DIR']+"/exiobase3/original-files/ixi/IOT_{year}_ixi.zip").year
ALL_YEARS_EUREGIO = glob_wildcards(config['MRIO_DATA_DIR']+"/euregio/original-files/EURegionalIOtable_{year}.ods").year

ALL_FULL_EXIO = expand("{outputdir}/exiobase3/builded-files/pkls/exiobase3_{year}_full.pkl",outputdir=config["MRIO_DATA_DIR"], year=ALL_YEARS_EXIO)
ALL_74_EXIO = expand("{outputdir}/exiobase3/builded-files/pkls/exiobase3_{year}_74_sectors.pkl",outputdir=config["MRIO_DATA_DIR"], year=ALL_YEARS_EXIO)
MINIMAL_7_EXIO = expand("{outputdir}/exiobase3/builded-files/pkls/exiobase3_2020_7_sectors.pkl",outputdir=config["MRIO_DATA_DIR"])

ALL_FULL_EUREGIO = expand("{outputdir}/euregio/builded-files/pkls/euregio_{year}_full.pkl",outputdir=config["MRIO_DATA_DIR"], year=ALL_YEARS_EUREGIO)

ALL_74_PARAMS_EXIO = expand("{outputdir}/exiobase3/builded-files/params/exiobase3_{sector_aggreg_name}_params.json",outputdir=config["MRIO_DATA_DIR"], sector_aggreg_name=["74_sectors"])

rule minimal_exio:
    input:
        MINIMAL_7_EXIO

rule all_full_exio:
    input:
        ALL_FULL_EXIO

rule all_74_sect_exio:
    input:
        mrios=ALL_74_EXIO,
        param=ALL_74_PARAMS_EXIO

rule all_full_euregio:
    input:
        ALL_FULL_EUREGIO

rule generate_subregions_mrio:
    input:
        in_mrio = expand("{outputdir}/{{mrio_basename}}/builded-files/pkls/{{sector_aggreg_name}}_{{region_aggreg_name}}.pkl",outputdir=config["MRIO_DATA_DIR"]),
        #mrio_params = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}_params.json",outputdir=config["BUILDED-FILES_DATA_DIR"])
    output:
        out_mrio = expand("{outputdir}/{{mrio_basename}}/builded-files/pkls/{{sector_aggreg_name}}_{{region_aggreg_name}}_{{subregions}}.pkl",outputdir=config["MRIO_DATA_DIR"]),
        #out_params = expand("{outputdir}/mrios/{{sector_aggreg_name}}_{{region_aggreg_name}}_{{subregions}}_params.json",outputdir=config["BUILDED_DATA_DIR"])
    conda:
        "boario-use"
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

rule generate_exiobase3_full_from_zip:
    input:
        mrio_file = expand("{inputdir}/exiobase3/original-files/ixi/IOT_{{year}}_ixi.zip",inputdir=config["MRIO_DATA_DIR"])
    #params:
    #    full_mrio_params = expand("{outputdir}/mrios/exiobase3_full_params.json",outputdir=config["BUILDED_DATA_DIR"])
    conda:
        "boario-use"
    output:
        mrioout = expand("{outputdir}/exiobase3/builded-files/pkls/exiobase3_{{year}}_full.pkl",outputdir=config["MRIO_DATA_DIR"])
    resources:
        vmem_mb=8000,
        mem_mb=7000,
        disk_mb=2000
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/build_pkl.py -t "EXIO3" -o {output.mrioout} -i {input.mrio_file};
        """

rule create_euregio_xlsx:
    input:
        expand("{folder}/euregio/original-files/EURegionalIOtable_{{year}}.ods",folder=config["MRIO_DATA_DIR"])
    output:
        expand("{folder}/euregio/builded-files/EURegionalIOtable_{{year}}.xlsx",folder=config["MRIO_DATA_DIR"])
    params:
        folder = f"{config['MRIO_DATA_DIR']}/euregio"
    run:
        folder = params.folder
        inpt = input
        print(f"Executing libreoffice --convert-to xlsx --outdir {folder}/builded-files {inpt}")
        os.system(f"libreoffice --convert-to xlsx --outdir {folder}/builded-files {inpt}")

rule create_euregio_csvs:
    input:
        expand("{folder}/euregio/builded-files/EURegionalIOtable_{{year}}.xlsx",folder=config["MRIO_DATA_DIR"])
    output:
        expand("{folder}/euregio/builded-files/euregio_{{year}}.csv",folder=config["MRIO_DATA_DIR"])
    params:
        folder = expand("{folder}/euregio",folder=config["MRIO_DATA_DIR"])
    shell:
        """
        xlsx2csv -s 3 {input} {output}
        """

rule generate_euregio_full_from_csv:
    input:
        io_files = expand("{inputdir}/euregio/builded-files/euregio_{{year}}.csv",inputdir=config["MRIO_DATA_DIR"]),
        index_files = expand("{inputdir}/euregio/{filenames}_index.csv",inputdir=config["MRIO_DATA_DIR"],filenames=["fd"])
    conda:
        "boario-use"
    output:
        mrioout = expand("{outputdir}/euregio/builded-files/pkls/euregio_{{year}}_full.pkl",outputdir=config["MRIO_DATA_DIR"])
    resources:
        vmem_mb=8000,
        mem_mb=7000,
        disk_mb=2000
    shell:
        """
        python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/create_euregio_csvs.py --input-folder {config[MRIO_DATA_DIR]}/euregio --year {wildcards.year} --output {output}
        """

rule mrio_params_build:
    input:
        ods = expand("{folder}/{{mrio_basename}}/params-files/{{mrio_basename}}_{{sector_aggreg_name}}_params.ods",folder=config["MRIO_DATA_DIR"])
    params:
        monetary = 1000000,
        main_inv_duration = 90,
        name_json = expand("{outputdir}/{{mrio_basename}}/builded-files/params/exiobase3_{{sector_aggreg_name}}_event_params.json",outputdir=config["MRIO_DATA_DIR"])
    resources:
        vmem_mb=1000,
        mem_mb=1000,
        disk_mb=1000
    wildcard_constraints:
        mrio_basename="euregio|exiobase3|wiod|icio|eora26",
        sector_aggreg_name="full|\d+_sectors"
    output:
        mrio_params_json = expand("{outputdir}/{{mrio_basename}}/builded-files/params/exiobase3_{{sector_aggreg_name}}_params.json",outputdir=config["MRIO_DATA_DIR"]),
        event_params_json = expand("{outputdir}/{{mrio_basename}}/builded-files/params/exiobase3_{{sector_aggreg_name}}_event_params_{evtype}.json",outputdir=config["MRIO_DATA_DIR"],evtype=["recover","rebuilding"])
    shell:
        """
        nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/mrio_json_params_build.py {input.ods} {params.monetary} {params.main_inv_duration} -po {output.mrio_params_json} -eo {params.name_json}
        """

rule mrio_sector_aggreg:
    input:
        full_mrio_file = expand("{outputdir}/{{mrio_basename}}/builded-files/pkls/{{mrio_basename}}_{{year}}_full.pkl",outputdir=config["MRIO_DATA_DIR"]),
        sector_aggreg_file = expand("{folder}/{{mrio_basename}}/aggreg-files/{{mrio_basename}}_{{sector_aggreg_name}}.ods",folder=config["MRIO_DATA_DIR"])
    params:
        full_mrio_params = expand("{folder}/{{mrio_basename}}/aggreg-files/{{mrio_basename}}_{{sector_aggreg_name}}_params.ods",folder=config["MRIO_DATA_DIR"])
    conda:
        "../env/boario-use.yml"
    output:
        out_mrio = expand("{folder}/{{mrio_basename}}/builded-files/pkls/{{mrio_basename}}_{{year}}_{{sector_aggreg_name}}.pkl",folder=config["MRIO_DATA_DIR"])
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
    # CHECK WILDCARD_CONSTRAINTS if problems
    input:
        mrio_file = expand("{outputdir}/{{mrio_basename}}/builded-files/pkls/{{mrio}}_FullWorld.pkl",outputdir=config["MRIO_DATA_DIR"]),
        region_aggreg_file = expand("{inputdir}/aggreg-files/{{region}}_aggreg.json",inputdir=config["MRIO_DATA_DIR"]),
        old_mrio_params = expand("{outputdir}/{{mrio_basename}}/builded-files/params-files/{{mrio_basename}}_FullWorld_params.json",outputdir=config["MRIO_DATA_DIR"])
    conda:
         "../env/boario-use.yml"
    output:
        out_mrio = expand("{outputdir}/{{mrio}}_{{region}}-RoW.pkl",outputdir=config["BUILDED_DATA_DIR"]),
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
