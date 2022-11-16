#!/bin/bash

versa_python_path="/home/sjuhel/mambaforge/envs/versa/bin/python"
script_path="/data/sjuhel/BoARIO-inputs/scripts/exp_aggreg.py"
exp_dir="/data/sjuhel/Runs/BoARIO-runs/"
showHelp() {
# `cat << EOF` This means that cat should stop reading when EOF is detected
cat << EOF
Usage: $0 --type=<exp_type> --output=<output_dir> [ --phase=<phase> --psi=<psi> --semester ]

-h,                    --help                       Display help
-t [hist|proj],        --type [hist|proj]           Set the experience type
-o <dir>,              --output <dir>               Set the output directory to <dir>
-P [1-6],              --phase [1-6]                Run only phase <phase>
-Y,                    --psi [0.0,1.0]              Run only for this psi parameter
-S,                    --semester                   Run for semester disagregation


EOF
# EOF is found above and hence cat command stops reading. This is equivalent to echo but much neater when printing out.
}

# $@ is all command line parameters passed to the script.
# -o is for short options like -v
# -l is for long options with double dash like --version
# the comma separates different long options
# -a is for long options with single dash like -version
options=$(getopt -l "help,type:,output:,phase::,psi::,semester" -o "ht:o:P::Y::S" -a -- "$@")

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
    export semester=1
    ;;
-Y|--psi)
    export psi_set=1
    shift
    export psi_val="$1"
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
    flood_base_path="/data/sjuhel/BoARIO-inputs/source-data/dottori_clustered_hist_flood_withdmg.parquet"
    rep_events="/data/sjuhel/BoARIO-inputs/source-data/representative_events_1970_2005.parquet"
elif [[ "$exp_type" == "proj" ]]
then
    exp_path="${exp_dir}experience-flood-dottori-2016-2035"
    flood_base_path="/data/sjuhel/BoARIO-inputs/source-data/dottori_clustered_proj_flood_withdmg.parquet"
    rep_events="/data/sjuhel/BoARIO-inputs/source-data/representative_events_2016_2035.parquet"
fi

if [[ $phase_run -eq 1 ]]
then
   if [[ $semester -eq 1 ]]
   then
       if [[ $psi_set -eq 1 ]]
       then
            run=( "$versa_python_path $script_path --semester --phase $phase_id --psi $psi_val -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
       else
            run=( "$versa_python_path $script_path --semester --phase $phase_id -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
       fi
   else
       if [[ $psi_set -eq 1 ]]
       then
           run=( "$versa_python_path $script_path --phase $phase_id --psi $psi_val -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
       else
           run=( "$versa_python_path $script_path --phase $phase_id -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events" )
       fi
   fi
else
   if [[ $semester -eq 1 ]]
   then
       if [[ $psi_set -eq 1 ]]
       then
           run=( "$versa_python_path $script_path --semester --phase 1 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 2 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 3 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 4 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 5 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 6 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
       else
           run=( "$versa_python_path $script_path --semester --phase 1 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 2 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 3 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 4 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 5 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --semester --phase 6 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
       fi
  else
       if [[ $psi_set -eq 1 ]]
       then
           run=( "$versa_python_path $script_path --phase 1 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 2 -i --psi $psi_val  $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 3 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 4 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 5 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 6 -i --psi $psi_val $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
       else
           run=( "$versa_python_path $script_path --phase 1 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 2 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 3 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 4 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 5 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events"
                 "$versa_python_path $script_path --phase 6 -i $exp_path -B $flood_base_path -N $exp_type -o $output_dir -R $rep_events")
      fi
  fi
fi


rm aggreg_built.sh
echo "#!/bin/bash
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
