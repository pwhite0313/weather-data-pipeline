#!/usr/bin/env bash
# Day 3B in one script, run from CloudShell as the console login:
# budget, SSH key import, instance role, security group, the instance.
#
# Needs three variables set in the shell first:
#   MY_IP        the laptop's public IPv4 (curl https://checkip.amazonaws.com)
#   PUBLIC_KEY   the laptop's ~/.ssh/weather-ec2.pub, one line
#   ALERT_EMAIL  where the budget alerts go
#
# Every create is one shot. Re-running reports "already exists" for what is
# there and carries on; it never deletes anything.
set -u
REGION=us-east-2
: "${MY_IP:?set MY_IP}" "${PUBLIC_KEY:?set PUBLIC_KEY}" "${ALERT_EMAIL:?set ALERT_EMAIL}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "account $ACCOUNT, region $REGION, ssh from $MY_IP/32"

echo "== budget weather-hosting: 50 USD a month, alerts at 50, 80 and 100 percent"
aws budgets create-budget --account-id "$ACCOUNT" \
  --budget '{"BudgetName":"weather-hosting","BudgetLimit":{"Amount":"50","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}' \
  --notifications-with-subscribers "[
    {\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":50,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$ALERT_EMAIL\"}]},
    {\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":80,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$ALERT_EMAIL\"}]},
    {\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":100,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$ALERT_EMAIL\"}]}]" \
  && echo "budget created" || echo "budget: see message above (already exists is fine)"

echo "== key pair weather-ec2 (public half only; the private key never leaves the laptop)"
printf '%s\n' "$PUBLIC_KEY" > /tmp/weather-ec2.pub
aws ec2 import-key-pair --key-name weather-ec2 --public-key-material fileb:///tmp/weather-ec2.pub --region "$REGION" --query KeyName --output text

echo "== role weather-ec2-role: EC2 may assume it; it carries the day 1 write policy"
cat > /tmp/ec2_trust.json <<'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF
aws iam create-role --role-name weather-ec2-role --assume-role-policy-document file:///tmp/ec2_trust.json --query Role.Arn --output text
aws iam attach-role-policy --role-name weather-ec2-role --policy-arn "arn:aws:iam::$ACCOUNT:policy/weather-extractor-write"
aws iam create-instance-profile --instance-profile-name weather-ec2-role --query InstanceProfile.Arn --output text
aws iam add-role-to-instance-profile --instance-profile-name weather-ec2-role --role-name weather-ec2-role

echo "== security group weather-ec2-sg: port 22 from $MY_IP/32, nothing else inbound"
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text --region "$REGION")
SG=$(aws ec2 create-security-group --group-name weather-ec2-sg --description "weather airflow box: ssh from home only" --vpc-id "$VPC" --query GroupId --output text --region "$REGION" 2>/dev/null \
  || aws ec2 describe-security-groups --filters Name=group-name,Values=weather-ec2-sg --query 'SecurityGroups[0].GroupId' --output text --region "$REGION")
aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MY_IP/32" --region "$REGION" --query 'Return' --output text
echo "security group $SG in $VPC"

echo "== instance: Ubuntu 24.04 LTS, t3.medium, 20 GB gp3, role attached at launch"
AMI=$(aws ssm get-parameters --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query 'Parameters[0].Value' --output text --region "$REGION")
echo "ami $AMI"
sleep 10   # a new instance profile takes a few seconds to become usable
IID=$(aws ec2 run-instances --image-id "$AMI" --instance-type t3.medium --key-name weather-ec2 \
  --security-group-ids "$SG" --iam-instance-profile Name=weather-ec2-role \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=weather-airflow}]' \
  --query 'Instances[0].InstanceId' --output text --region "$REGION")
echo "instance $IID launching"
aws ec2 wait instance-running --instance-ids "$IID" --region "$REGION"
IP=$(aws ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text --region "$REGION")

echo "== on demand price for docs/hosting.md"
aws pricing get-products --service-code AmazonEC2 --region us-east-1 \
  --filters Type=TERM_MATCH,Field=instanceType,Value=t3.medium Type=TERM_MATCH,Field=regionCode,Value="$REGION" \
            Type=TERM_MATCH,Field=operatingSystem,Value=Linux Type=TERM_MATCH,Field=tenancy,Value=Shared \
            Type=TERM_MATCH,Field=preInstalledSw,Value=NA Type=TERM_MATCH,Field=capacitystatus,Value=Used \
  --query 'PriceList[0]' --output text 2>/dev/null \
  | python3 -c "import sys,json; p=json.loads(sys.stdin.read()); t=list(p['terms']['OnDemand'].values())[0]; d=list(t['priceDimensions'].values())[0]; print('t3.medium', d['pricePerUnit']['USD'], 'USD per hour')" \
  || echo "price lookup failed; read it from https://aws.amazon.com/ec2/pricing/on-demand/"

echo
echo "RESULT instance_id=$IID public_ip=$IP security_group=$SG"
echo "RESULT ssh -i ~/.ssh/weather-ec2 ubuntu@$IP"
