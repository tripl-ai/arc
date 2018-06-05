# set directory
mkdir -p ${ETL_CONF_BASE_URL}/data
cd ${ETL_CONF_BASE_URL}/data

# download all the easy files to versioned directories
cat /opt/tutorial/nyctaxi/raw_data_urls_small.txt | xargs -n1 mkdir -p
cat /opt/tutorial/nyctaxi/raw_data_urls_small.txt | xargs -n2 -P4 wget -c -P 

# create empty directory to demonstrate schema evolution
rm -R https\:/
mkdir -p uber_tripdata/1/