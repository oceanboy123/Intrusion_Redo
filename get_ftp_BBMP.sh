#!/bin/bash

# Define variables
FTP_SERVER="ftp.dfo-mpo.gc.ca"
FTP_USER="anonymous"
FTP_PASS="guest"
REMOTE_DIR="/BIOWebMaster/BBMP/CSV"
FILE_NAME="bbmp_aggregated_profiles.csv"

ftp -inv ftp.dfo-mpo.gc.ca <<EOF
user $FTP_USER 
pass $FTP_PASS
cd $REMOTE_DIR
get $FILE_NAME
bye
EOF

echo "Download completed. File saved as $FILE_NAME"