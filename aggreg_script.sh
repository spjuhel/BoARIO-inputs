#!/bin/bash

versa_python_path="/home/sjuhel/mambaforge/envs/versa/bin/python"
script_path="/data/sjuhel/BoARIO-inputs/scripts/exp_aggreg.py"
exp_dir="/data/sjuhel/Runs/BoARIO-runs/"
flood_prot="/data/sjuhel/BoARIO-inputs/source-data/FLOPROS_shp_V1/"

showHelp() {
    # `cat << EOF` This means that cat should stop reading when EOF is detected
    cat << EOF
Usage: $0 --type=<exp_type> --output=<output_dir> [ --phase=<phase> --psi=<psi> --custom-run=<dir> --semester=<max_sem> ]

-h,                    --help                              Display help
-t [hist|proj],        --type [hist|proj1|proj2]           Set the experience type
-o <dir>,              --output <dir>                      Set the output directory to <dir>
-P [1-6],              --phase [1-6]                       Run only phase <phase>
-Y,                    --psi [0.0,1.0]                     Run only for this psi parameter
-C,                    --custom-run <dir>                  Run for a non default simulation
-S [1-8],              --semester <max_sem>                Run with semester disagregation


EOF
    # EOF is found above and hence cat command stops reading. This is equivalent to echo but much neater when printing out.
}

# $@ is all command line parameters passed to the script.
# -o is for short options like -v
# -l is for long options with double dash like --version
# the comma separates different long options
# -a is for long options with single dash like -version
options=$(getopt -l "help,type:,output:,phase::,psi::,custom-run::,semester::" -o "ht:o:P::Y::C::S::" -a -- "$@")

# set --:
# If no arguments follow this option, then the positional parameters are unset. Otherwise, the positional parameters
# are set to the arguments, even if some of them begin with a ‘-’.
eval set -- "$options"

while true
do
    case "$1" in
        -h|--help)
            showHelp
            exit 0
            ;;
        -t|--type)
            shift
            export exp_type="$1"
            ;;
        -o|--output)
            shift
            export output_dir="$1"
            ;;
        -P|--phase)
            export phase_run=1
            shift
            export phase_id="$1"
            ;;
        -S|--semester)
            export semester_run=1
            shift
            export semester="$1"
            ;;
        -Y|--psi)
            export psi_set=1
            shift
            export psi_val="$1"
            ;;
        -C|--custom-run)
            export custom_run=1
            shift
            export custom_dir="$1"
            ;;
        --)
            shift
            break;;
    esac
    shift
done

if [[ "$exp_type" == "hist" ]]
then
    exp_path="${exp_dir}experience-flood-dottori-1970-2005";
    flood_base_path="/data/sjuhel/BoARIO-inputs/source-data/full_floodbase_1970_2005.parquet"
    rep_events="/data/sjuhel/BoARIO-inputs/source-data/representative_events_1970_2005.parquet"
elif [[ "$exp_type" == "proj1" ]]
then
    exp_path="${exp_dir}experience-flood-dottori-2016-2035"
    flood_base_path="/data/sjuhel/BoARIO-inputs/source-data/full_floodbase_2016_2035.parquet"
    rep_events="/data/sjuhel/BoARIO-inputs/source-data/representative_events_2016_2035.parquet"
elif [[ "$exp_type" == "proj2" ]]
then
    exp_path="${exp_dir}experience-flood-dottori-2035-2050"
    flood_base_path="/data/sjuhel/BoARIO-inputs/source-data/full_floodbase_2035_2050.parquet"
    rep_events="/data/sjuhel/BoARIO-inputs/source-data/representative_events_2035_2050.parquet"
fi

if [[ $custom_run -eq 1 ]]
then
    exp_path="${custom_dir}"
fi

if [[ $phase_run -eq 1 ]]
then
    if [[ $semester_run -eq 1 ]]
    then
        if [[ $psi_set -eq 1 ]]
        then
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase $phase_id --psi $psi_val -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
        else
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase $phase_id -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
        fi
    else
        if [[ $psi_set -eq 1 ]]
        then
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --phase $phase_id --psi $psi_val -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
        else
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --phase $phase_id -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
        fi
    fi
else
    if [[ $semester_run -eq 1 ]]
    then
        if [[ $psi_set -eq 1 ]]
        then
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 1 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 2 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 3 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 4 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 5 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 6 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
        else
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 1 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 2 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 3 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 4 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 5 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --semester $semester --phase 6 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
        fi
    else
        if [[ $psi_set -eq 1 ]]
        then
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 1 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 2 -i --psi $psi_val  $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 3 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 4 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 5 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 6 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
        else
            run=( "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 1 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 2 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 3 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 4 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 5 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                  "$versa_python_path $script_path --protection-dataframe $flood_prot --phase 6 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
        fi
    fi
fi


rm aggreg_built.sh

echo "#!/bin/bash
#SBATCH --mail-type=END,FAIL                   # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=sjuhel@centre-cired.fr
#SBATCH --job-name=Result_aggreg.job
#SBATCH --output=/data/sjuhel/Runs/res_aggreg.out
#SBATCH --error=/data/sjuhel/Runs/res_aggreg.err
#SBATCH --time=1-00:00
#SBATCH --partition=zen16
#SBATCH --ntasks 4
" >>  "aggreg_built.sh"

for cmd in "${run[@]}"; do
    echo "$cmd" >> "aggreg_built.sh"
done
