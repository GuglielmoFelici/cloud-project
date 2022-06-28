for (( i=1; i<$1; i++ )); do
    aws lambda invoke --function-name sensors --payload '{  }' response.json
    sleep $2
done