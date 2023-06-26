curl https://repo.anaconda.com/archive/Anaconda3-2023.03-1-Linux-x86_64.sh --output ~/Downloads/anaconda.sh
sha256sum ~/Downloads/anaconda.sh
bash ~/Downloads/anaconda.sh
conda create -n .condaenv python=3.10 scipy=0.15.0 numpy pandas matplotlib, plotly