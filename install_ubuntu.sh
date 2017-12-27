sudo apt-get update
sudo apt-get install python
sudo apt-get install python-setuptools
sudo apt-get install python-pip
export LC_ALL=C
sudo pip install --no-cache-dir -r requirements.txt
sudo pip install Cython
sudo python setup.py build_ext --inplace