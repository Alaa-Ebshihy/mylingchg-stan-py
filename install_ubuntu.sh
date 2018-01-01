sudo apt-get update

sudo apt-get install python
sudo apt-get install python-setuptools

#install and upgrade pip
sudo apt-get install python-pip
pip install --upgrade pip
echo 'export LC_ALL=C' >> ~/.bashrc
. ~/.bashrc

#install requirements
sudo pip install --no-cache-dir -r requirements.txt
sudo pip install Cython

#build cython modules
sudo python setup.py build_ext --inplace

##update PYTHONPATH env variable
echo 'export PYTHONPATH=/implementation/mylingchg-stan-py' >> ~/.bashrc
echo 'export PYTHONPATH=$PYTHONPATH:/implementation/mylingchg-stan-py/googlengram' >> ~/.bashrc
. ~/.bashrc