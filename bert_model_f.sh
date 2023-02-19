#!/bin/bash
#SBATCH --partition=compute
#SBATCH --cpus-per-task=48
#SBATCH --job-name=bert-job
#SBATCH --time=24:00:00
#SBATCH --mem=150GB
#SBATCH --mail-user=fortuzoc98@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --output=%x-%j.log

source myproject_env/bin/activate 


python bert_f.py