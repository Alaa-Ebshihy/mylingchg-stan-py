sudo apt-get update

#for multiple processes
sudo apt-get install parallel

sudo apt-get install python
sudo apt-get install python-setuptools

#install and upgrade pip
sudo apt-get install python-pip
pip install --upgrade pip
export LC_ALL=C
echo 'export LC_ALL=C' >> ~/.bashrc

#install requirements
sudo pip install --no-cache-dir -r requirements.txt
sudo pip install Cython
pip install rpy2

#build cython modules
sudo python setup.py build_ext --inplace

##update PYTHONPATH env variable
echo 'export PYTHONPATH=/implementation/mylingchg-stan-py' >> ~/.bashrc
echo 'export PYTHONPATH=$PYTHONPATH:/implementation/mylingchg-stan-py/googlengram' >> ~/.bashrc
#after finishing install call the following
#. ~/.bashrc