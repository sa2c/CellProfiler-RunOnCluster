template_string = '''#!/bin/bash

#SBATCH --job-name=${jobname}
#SBATCH --output=${submit_script_dir}/${jobname}.submit.stdout
#SBATCH --error=${submit_script_dir}/${jobname}.submit.stderr
#SBATCH --nodes=${nodes}
#SBATCH --partition=${partition}
#SBATCH --time=${walltime}
#SBATCH --ntasks-per-node=${tasks_per_node}

$overrides

export JOBNAME="${jobname}"

$user_script
'''
