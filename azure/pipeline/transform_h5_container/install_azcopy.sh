
#!/bin/bash

# Install AzCopy on Linux

# Download and extract
wget https://aka.ms/downloadazcopy-v10-linux
tar -xvf downloadazcopy-v10-linux

# Move AzCopy
rm -f /usr/bin/azcopy
cp ./azcopy_linux_amd64_*/azcopy /usr/bin/
chmod 755 /usr/bin/azcopy

# Clean the kitchen
rm -f downloadazcopy-v10-linux
rm -rf ./azcopy_linux_amd64_*/
