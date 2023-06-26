set -o verbose
##################################
#### # SETTING VS CODE VENV # ####
##################################
# step-00 - pre-requisite: create a virtual environment
sudo apt update
sudo apt install python3.10-venv
python3 -m venv .venv
source .venv/bin/activate
# these are the extensions i have installed in my vscode
#code --list-extensions --show-versions > requirements.txt
pip install -r requirements/pip_requirements.txt
# installing vs code extensions
while IFS="" read -r p || [ -n "$p" ]
do
  code --install-extension "$p"
done < requirements/vscode_extensions.txt
#############################
### # INSTALLTING SPARK # ###
#############################
# step-01 - 1 - installing jdk
cd $HOME
sudo apt install default-jre default-jdk
# step-01 - 2 - check java version
java -version
# step-02 - 1 - downloading latest spark
curl --create-dirs --output Downloads/spark-3.4.1-bin-hadoop3.tgz https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
cd $HOME/Downloads
tar -xzf spark-3.4.1-bin-hadoop3.tgz
# step-03 - set spark environment
sudo mv $HOME/Downloads/spark-3.4.1-bin-hadoop3 /opt/spark-3.4.1
sudo ln -s /opt/spark-3.4.1 /opt/spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
# step-04 - install FindSpark package
pip install findspark