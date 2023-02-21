def which_runs(wildcards):
    """
    Depending on the experience, there may be 'intensity' runs or 'raw' (damages) run, this
    """
    if wildcards.run_type=="raw":
        return RAW_DMG_RUNS
    elif wildcards.run_type=="int":
        return RUNS
    elif wildcards.run_type=="all":
        return RAW_DMG_RUNS + RUNS
    else:
        raise ValueError("Unrecognized run_type.")

rule generate_csv:
    input:
        which_runs
    output:
        expand("{outputdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], files=["{run_type}_dmg_general", "{run_type}_dmg_prodloss","{run_type}_dmg_fdloss"])
    shell:
    """
    nice -n 10 python {config[INPUTS_GENERATION_SCRIPTS_DIR]}/csv_from_indicators.py {config[OUTPUT_DIR]} {wildcards.run_type} -o {config[OUTPUT_DIR]}
    """

rule all_xp:
    input:
        RUNS

rule csv_raw:
    """

    """
    input:
        expand("{outputdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], files=["raw_dmg_general", "raw_dmg_prodloss","raw_dmg_fdloss"])

rule csv_int:
    input:
        expand("{outputdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], files=["int_dmg_general", "int_dmg_prodloss","int_dmg_fdloss"])

rule all_csv:
    input:
        expand("{outputdir}/{files}.csv", outputdir=config["OUTPUT_DIR"], files=["all_dmg_general", "all_dmg_prodloss","all_dmg_fdloss"])

rule run_subregions_mrio:
    """
    Run simulation where one region has been cut in subregions.
    (Probably deprecated)
    """
    input:
        unpack(run_inputs)
    output:
        expand("{out}/{{xp_folder}}/{{mrio_used}}/{{region}}_type_Subregions_qdmg_{{run_type}}_{{flood}}_Psi_{{psi}}_inv_tau_{{inv}}_inv_time_{{inv_t}}/{files}",
               out=config["OUTPUT_DIR"],
               files=run_output_files)
    params:
        output_dir = config["OUTPUT_DIR"],
        event_template = lambda wildcards : get_event_template(wildcards.mrio_used,wildcards.xp_folder),
        mrio_params = lambda wildcards : get_mrio_params(wildcards.mrio_used,wildcards.xp_folder)
    threads:
        8
    resources:
        vmem_mb=3000,
        mem_mb=2000,
        disk_mb=500
    wildcard_constraints:
        region="(?:[A-Z]{2}-[A-Z]{2}\d+)|([A-Z]{2}-all)",
        mrio_used="exiobase3_(?:Full|\d+_sectors)_(?:FullWorld|[A-Z]{2}-RoW)_[A-Z]{2}_sliced_in_\d+"
    conda:
        "env/boario-use.yml"
    params:
        output_dir = config["OUTPUT_DIR"],
    shell:
        """
        cd {config[ARIO_DIR]};
        nice -n 10 python ./scripts/mono_run.py {wildcards.region} {input.params_template} {wildcards.psi} {wildcards.inv} Full {wildcards.run_type} {wildcards.flood} {input.mrio} {params.output_dir}/{wildcards.xp_folder}/{wildcards.mrio_used} {input.flood_gdp} {params.event_template} {params.mrio_params} {wildcards.inv_t}
        """

rule run_RoW:
    """
    Run 'Rest of the World' simulation, where all regions expect the chosen one are agregated into a RoW region. (For testing/sensitivity purpose, might be broken at the moment)
    """
    input:
        unpack(run_RoW_inputs)
    output:
        expand("{out}/{{xp_folder}}/{{mrio_used}}/{{region}}_type_RoW_qdmg_{{run_type}}_{{flood}}_Psi_{{psi}}_inv_tau_{{inv}}_inv_time_{{inv_t}}/{files}",
               out=config["OUTPUT_DIR"],
               files=run_output_files)
    conda:
        "env/boario-use.yml"
    wildcard_constraints:
        run_type="raw|int"
    resources:
        mem_mb=4000,
        disk_mb=50,
        vmem_mb=4000
    params:
        ario_dir = config["ARIO_DIR"],
        output_dir = config["OUTPUT_DIR"],
    shell:
        """
        cd {params.ario_dir};
        nice -n 10 python ./scripts/mono_run.py {wildcards.region} {input.params_template} {wildcards.psi} {wildcards.inv} RoW {wildcards.run_type} {wildcards.flood} {input.mrio} {params.output_dir}/{wildcards.xp_folder}/{wildcards.mrio_used} {input.flood_gdp} {input.event_template} {input.mrio_params} {wildcards.inv_t}
        """

rule run_Full:
    """
    Run a "Full" simulation (no mrio region agregation or disagregation).

    run_inputs(wildcards) is in common.smk
    """
    input:
        unpack(run_inputs),
        event_template = lambda wildcards : get_event_template(wildcards.mrio_used,wildcards.shock_type),
        mrio_params = lambda wildcards : get_mrio_params(wildcards.mrio_used,wildcards.xp_folder)
    output:
        record_files =  temp(directory(expand("{out}/{{xp_folder}}/{{mrio_used}}/{{region}}_type_{{shock_type}}_qdmg_{{run_type}}_{{flood}}_Psi_{{psi}}_inv_tau_{{inv}}_inv_time_{{inv_t}}/records", out=config["OUTPUT_DIR"]))),
        json_files = directory(expand("{out}/{{xp_folder}}/{{mrio_used}}/{{region}}_type_{{shock_type}}_qdmg_{{run_type}}_{{flood}}_Psi_{{psi}}_inv_tau_{{inv}}_inv_time_{{inv_t}}/jsons", out=config["OUTPUT_DIR"]))
    #group: "sim_run"
    params:
        output_dir = config["OUTPUT_DIR"]
    threads:
        4
    log:
        expand("{out}/{{xp_folder}}/{{mrio_used}}/{{region}}_type_{{shock_type}}_qdmg_{{run_type}}_{{flood}}_Psi_{{psi}}_inv_tau_{{inv}}_inv_time_{{inv_t}}/simulation.log",
               out=config["OUTPUT_DIR"])
    resources:
        vmem_mb=run_Full_get_vmem_mb,
        mem_mb=run_Full_get_mem_mb,
        disk_mb=run_Full_get_disk_mb
    wildcard_constraints:
        region="[A-Z]{2}"
    conda:
        "env/boario-use.yml"
    shell:
        """
        cd {config[ARIO_DIR]};
        nice -n 10 python ./scripts/mono_run.py {wildcards.region} {input.params_template} {wildcards.psi} {wildcards.inv} {wildcards.shock_type} {wildcards.run_type} {wildcards.flood} {input.mrio} {params.output_dir}/{wildcards.xp_folder}/{wildcards.mrio_used} {input.flood_gdp} {input.event_template} {input.mrio_params} {wildcards.inv_t}
        """
