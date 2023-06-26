BLUE='\033[0;34m'
RED='\033[0;31m'
GREEN='\033[0;32m'
if [ -f "/etc/debian_version" ]; then
  printf "\n${GREEN}This is debian OS"
  ##################################
  #### # SETTING VS CODE VENV # ####
  ##################################
  # step-00 - pre-requisite: create a virtual environment
  printf "\n${BLUE}Updating apt-get\n"; sleep 2
  sudo apt update > /dev/null 2>&1
  read -r -p "\nAre you running this project in vs code? [y/N]" response
  if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]
  then
      printf "\n${BLUE}Installing python3.10 virtual environment\n"; sleep 2
      sudo apt install python3.10-venv > /dev/null 2>&1
      printf "\n${BLUE}Creating local virtual environment\n"; sleep 2
      python3 -m venv .venv > /dev/null 2>&1
      printf "\n${BLUE}Activating local virtual environment, Any installation henceforth would be in local .venv folder\n"; sleep 2
      source .venv/bin/activate > /dev/null 2>&1
      ##########################################
      #### # INSTALLING REQUIRED PRE_REQS # ####
      ##########################################
      # installing python pip packages
      printf "\n${BLUE}Installating python pip packages needed\n"; sleep 2
      pip install -r requirements/pip_requirements.txt > /dev/null 2>&1
      # installing vs code extensions
      printf "\n${BLUE}Installating vs code extensions\n"; sleep 2
      while IFS="" read -r p || [ -n "$p" ]
      do
        code --install-extension "$p"
      done < requirements/vscode_requirements.txt
      #############################
      ### # INSTALLTING SPARK # ###
      #############################
      # step-01 - 1 - installing jdk
      printf "\n${BLUE}Installating latest (default) jdk / jre needed for spark\n"; sleep 2
      cd $HOME
      sudo apt install default-jre default-jdk > /dev/null 2>&1
      # step-01 - 2 - check java version
      printf "\n${BLUE}java version check, if this returns value, java is detected and working!\n"; sleep 2
      java -version
      # step-02 - 1 - downloading latest spark
      printf "\n${BLUE}Downloading spark 3.4.1!\n"; sleep 1
      read -r -p "${RED}\nThis will delete the file 'spark-3.4.1-bin-hadoop3.tgz' if already exists at ${HOME}/Downloads, proceed? [y/n]" response
      if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]
      then
        rm $HOME/Downloads/spark-3.4.1-bin-hadoop3.tgz 2> /dev/null
        curl --create-dirs --output $HOME/Downloads/spark-3.4.1-bin-hadoop3.tgz https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
      else
        printf "\n${RED}The delete and re-download of spark was disallowed, exiting!\n"
        exit 1;
      fi
      cd $HOME/Downloads
      printf "\n${BLUE}Unpacking spark 3.4.1!\n"; sleep 1
      tar -xzf spark-3.4.1-bin-hadoop3.tgz
      # step-03 - set spark environment
      printf "\n${BLUE}moving unpacked content to /opt/spark-3.4.1\n"; sleep 1
      sudo mv $HOME/Downloads/spark-3.4.1-bin-hadoop3 /opt/spark-3.4.1 > /dev/null 2>&1
      printf "\n${BLUE}linking /opt/spark-3.4.1 to default spark at /opt/spark\n"; sleep 1
      sudo ln -s /opt/spark-3.4.1 /opt/spark > /dev/null 2>&1
      printf "\n${BLUE}Putting '/opt/spark' to bashrc PATH\n"; sleep 1
      PATH=$(echo "$PATH" | sed -e 's/:\/opt\/spark$//')
      export SPARK_HOME=/opt/spark > /dev/null 2>&1
      export PATH=$SPARK_HOME/bin:$PATH > /dev/null 2>&1
      # step-04 - install FindSpark package
      printf "\n${BLUE}Installing python3 pip 'findspark' package\n"; sleep 1
      pip install findspark > /dev/null 2>&1
      exit 0;
  else
      printf "\n${RED}The installation steps are only for vs code setup\n"
      exit 1;
  fi
  
else
   printf "\n${RED}This script is only for debian OS\n"
   exit 1;
fi
