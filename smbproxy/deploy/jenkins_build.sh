#!/usr/bin/env bash

set -ex

JENKINS_PIPELINE="python ${WORKSPACE}/jenkins_pipeline.py"

${JENKINS_PIPELINE} setup-pypi-mirror

# Create the base directory to do the deployment
sudo rm -rf /home/smbproxy4
sudo mkdir /home/smbproxy4
sudo chown -R jenkins:jenkins /home/smbproxy4

# Create the virtualenv
PYENV_HOME=/home/smbproxy4/venv/
${JENKINS_PIPELINE} setup-venv "${PYENV_HOME}"
. ${PYENV_HOME}/bin/activate

# Install necessary packages
pip install --quiet nosexcover pytest pytest-cov
${JENKINS_PIPELINE} install-from-setup-py
#nosetests --with-xcoverage --with-xunit --cover-package=smbproxy4 --cover-erase -a cat=unit
export PYTHONPATH='.'
py.test
python setup.py bdist_egg || exit 1

# Create sloccount report
${JENKINS_PIPELINE} sloccount-report sloccount.sc smbproxy4


# Put the various additional files in the package
cp dist/smbproxy-4.0.3-py2.7.egg /home/smbproxy4/smbproxy.egg
cp deploy/smbproxy.sh /home/smbproxy4/smbproxy.sh
cp deploy/check_smbproxy.sh /home/smbproxy4/check_smbproxy.sh
cp deploy/metadata_proxy.sh /home/smbproxy4/metadata_proxy.sh
cp -r nagios /home/smbproxy4/
chmod +x /home/smbproxy4/smbproxy.sh
chmod +x /home/smbproxy4/check_smbproxy.sh
chmod +x /home/smbproxy4/metadata_proxy.sh

rm -f smbproxy*.deb
/usr/local/bin/fpm -n smbproxy -v 0.1.${BUILD_NUMBER} -s dir -t deb --deb-user root --deb-group root \
    /home/smbproxy4
mv smbproxy*.deb smbproxy.deb