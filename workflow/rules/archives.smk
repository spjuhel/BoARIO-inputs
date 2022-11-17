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
