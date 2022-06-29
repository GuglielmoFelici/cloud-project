TABLE_NAME="measurements6"
read -p "Do you want to create the table? [y/n] " create
if [[ $create == 'y' ]]; then
    aws dynamodb create-table \
    --table-name $TABLE_NAME \
    --attribute-definitions \
    AttributeName=dynamo_id,AttributeType=S \
    --key-schema \
    AttributeName=dynamo_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    && echo "Table created" || exit
fi

read -p "Do you want to download the data? [y/n] " download_file
if [[ $download_file == 'y' ]]; then
    curl https://raw.githubusercontent.com/GuglielmoFelici/cloud-project/master/mock-data/results/2022-06-29_23-09.txt -o results.txt || exit
fi
if [[ $create == 'y' || $download_file == 'y' ]]; then 
    echo
    echo "You can call write_data.sh to write the downloaded data in the created table"
fi