docker build -t transform_h5_container -f ./pipeline/transform_h5_container/Dockerfile .
docker tag transform_h5_container:latest 351672045885.dkr.ecr.us-west-2.amazonaws.com/transform_h5_container:latest
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 351672045885.dkr.ecr.us-west-2.amazonaws.com
docker push 351672045885.dkr.ecr.us-west-2.amazonaws.com/transform_h5_container:latest
