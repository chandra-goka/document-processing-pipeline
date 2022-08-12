cd ../code
bash build.sh
cd ../infrastructure
cdk bootstrap
cdk deploy --all