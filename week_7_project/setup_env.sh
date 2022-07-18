sudo apt update
sudo apt -y update
sudo apt install wget
sudo apt install unzip
sudo apt install git
sudo apt install docker.io
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart
cd ~
mkdir bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.6.1/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose
#Add to ~/.bashrc: export PATH="${HOME}/bin:${PATH}"
#source .bashrc
wget https://releases.hashicorp.com/terraform/1.2.4/terraform_1.2.4_linux_amd64.zip
unzip terraform_1.2.4_linux_amd64.zip
rm terraform_1.2.4_linux_amd64.zip
