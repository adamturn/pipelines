echo RunMeFirst.sh has started!

source /root/miniconda3/etc/profile.d/conda.sh

echo RunMeFirst: \# conda activate googleapi
conda activate googleapi

echo RunMeFirst: \# python /some/dir/loader_module/loader_module.py
python /some/dir/loader_module/loader_module.py

echo RunMeFirst: \# conda deactivate
conda deactivate

echo RunMeFirst.sh has finished!