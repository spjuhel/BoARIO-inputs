def run_Full_get_mem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return max(2000*n_2years,4000)

def run_Full_get_vmem_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return max(3000*n_2years,6000)

def run_Full_get_disk_mb(wildcards, input):
    run_config = Path(str(input.params_template))
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 500*n_2years

def indicators_get_mem_mb(wildcards, input):
    #run_config = (Path(input[0]).parent)/"simulated_params.json"
    #with run_config.open("r") as f:
    #    params_template = json.load(f)

    #n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 8000

def indicators_get_vmem_mb(wildcards, input):
    #run_config = (Path(input[0]).parent)/"simulated_params.json"
    #with run_config.open("r") as f:
    #    params_template = json.load(f)

    #n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 7000 #*n_2years

def indicators_get_disk_mb(wildcards, input):
    run_config = (Path(input[0]).parent)/"simulated_params.json"
    with run_config.open("r") as f:
        params_template = json.load(f)

    n_2years = params_template["n_temporal_units_to_sim"] // 730
    return 500*n_2years
