#!/bin/bash

git config --global user.name "Darren Foley"
git config --global user.email "darren.foley@ucdconnect.ie"

SSH_DIR=/root/.ssh

[ ! -d "${SSH_DIR}" ] && mkdir -p "${SSH_DIR}" && echo "SSH  Directory Created"

sudo apt-get install -y openssh-client

cd $SSH_DIR

ssh-keygen -f $SSH_DIR/id_rsa -t rsa -b 4096
