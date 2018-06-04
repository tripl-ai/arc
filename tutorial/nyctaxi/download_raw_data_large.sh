# set directory
cd ${ETL_CONF_BASE_URL}

# download all the easy files to versioned directories
cat /opt/tutorial/nyctaxi/raw_data_urls_large.txt | xargs -n1 mkdir -p
cat /opt/tutorial/nyctaxi/raw_data_urls_large.txt | xargs -n2 wget -c -P 

# create empty directory to demonstrate schema evolution
rm -R https\:/
mkdir -p data/uber_tripdata/1/
