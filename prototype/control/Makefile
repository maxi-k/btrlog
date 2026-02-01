## local config files to bootstrap the cluster
LOCAL_AWS_CREDENTIALS := $(HOME)/.aws/credentials
LOCAL_AWS_CONFIG := $(HOME)/.aws/config
LOCAL_AWS_SSH_KEYNAME := aws-org-eu-central-1
# LOCAL_AWS_SSH_KEYNAME := aws-org-us-east-2
# LOCAL_AWS_SSH_KEYNAME := aws-org-ap-southeast-1
LOCAL_AWS_SSH_FILE ?= $(HOME)/.ssh/$(LOCAL_AWS_SSH_KEYNAME).pem

## key files for usage in terraform; linked
AWS_KEY_LOC ?= .env/aws.key
AWS_CONFIG_LOC ?= .env/aws.config
AWS_CREDENTIALS_LOC ?= .env/aws.credentials
AWS_KEYNAME_LOC ?= .env/aws.keyname

## terraform variables
tfvars ?= 

## -----------------------------------------------------------------------------
## AWS secrets configuration
## -----------------------------------------------------------------------------

$(AWS_CONFIG_LOC): $(LOCAL_AWS_CONFIG)
	@echo "Linking local aws config"
	@mkdir -p $(dir $@)
	@ln -sf $(LOCAL_AWS_CONFIG) $@

$(AWS_KEYNAME_LOC): $(LOCAL_AWS_SSH_FILE)
	@echo "Writing local aws keyname to file"
	@mkdir -p $(dir $@)
	@echo $(LOCAL_AWS_SSH_KEYNAME) > $@

$(AWS_KEY_LOC): $(LOCAL_AWS_SSH_FILE)
	@echo "Linking local aws key"
	@mkdir -p $(dir $@)
	@ln -sf $(LOCAL_AWS_SSH_FILE) $@

$(AWS_CREDENTIALS_LOC): $(LOCAL_AWS_CREDENTIALS)
	@echo "Linking local aws credentials"
	@mkdir -p $(dir $@)
	@ln -sf $(LOCAL_AWS_CREDENTIALS) $@

secrets: $(AWS_KEY_LOC) $(AWS_CREDENTIALS_LOC) $(AWS_KEYNAME_LOC) $(AWS_CONFIG_LOC)
	@echo "Creating secrets for terraform"
.PHONY: secrets

clean: stop-cluster
	rm -rf .env

## -----------------------------------------------------------------------------
## Cluster tasks
## -----------------------------------------------------------------------------

## start cluster using terraform
start-cluster: $(AWS_KEY_LOC)
	@echo "Starting cluster"
	@terraform init
	@terraform apply $(tfvars) -parallelism 4 #-auto-approve
	@notify-send "Cluster Started"
.PHONY: start-cluster

## "dry run" of the terraform plan
preview-cluster:
	@echo "Previewing cluster"
	@terraform plan $(tfvars)
.PHONY: preview-cluster

## stop cluster using terraform
stop-cluster:
	@echo "Stopping cluster"
	@terraform destroy -auto-approve -parallelism 4
.PHONY: stop-cluster
