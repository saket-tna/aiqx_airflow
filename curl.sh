curl --insecure -u "sharan@miqdigital.com:11680f86587021655cdc85fd8cf629fc72" https://builds-corp.mediaiqdigital.com/job/aiqx-POST-UNIV-SEARCH-HIVE/buildWithParameters\?token\=postAiqxUnivHiveJob\&daySerialNumeric\=$1\&branch\=master\&destination\=production

###running cache delete script
pip install requests
aws s3 cp s3://aiqdatabucket/scripts/aiqx/uns_qubole/uns_delete_cache.py ./
python uns_delete_cache.py