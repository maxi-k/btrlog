# ------------------------------------------------------------------------------
# AWS Setup
# ------------------------------------------------------------------------------
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "aws_sshkey_name" {
  description = "Path containing a file with the name of the SSH key to use for the instances"
  default = "./.env/aws.keyname"
}

variable "aws_sshkey_filepath" {
  description = "Path to SSH keys to SSH-connect to instances"
  default = "./.env/aws.key"
}

variable "aws_credentials_filepath" {
  description = "Path to aws credentials file"
  default = "./.env/aws.credentials"
}

variable "aws_config_filepath" {
  description = "Path to aws config file"
  default = "./.env/aws.config"
}

variable "aws_region" {
  description = "AWS region to use for the instances"
  #default = "us-east-2"
  default = "eu-central-1"
  #default = "ap-southeast-1"
}

variable "aws_az_suffixes" {
  description = "AWS availability zone suffixes to use for the instances"
  type = list(string)
  default = ["a", "b", "c"]
}

## instance type depends on architecture
##
## according to cloudspecs, we should choose c7gd;
## the best intel instance is c6id, which is ~10% worse.
## see
## - https://cloudspecs.fyi?state=N4IgzgjgNgigrgUwE4E8QC4QDEBKB5AWQAIBDAdzAH0AzEgWwEsoUAdAOwHUAJAURx6JgALgHskJAOYJKAYxFw2QogD4iABnYgANCCQBhEQBMEGEKMpgAbhKIAeALSDrwpAzYSAFGQaGhACyIAXiJ5IQAHOCEAOm9fPyi3GT8tIj8EBgk-JWDQiOi0jKyEtiSUsBkSKDcbYIBGAEp2dgBiVqIZOCQkBEUiCERUIm6wOCglBjAiNyIAckNqGfZcyLtHCQkwqBEhD3mUkgQwDwAPQOExSWkyVyFpBhEwsAB6MNcZaT95JBS2BCFKCQAIwY4WerwY70on06KSgJEBCCggTCJGECA8tEYzBSi3YMx+fwBwNBOIA4sSnmAZvUaUQANTsIhMohSER0Si3Y47er0xnM1nssIiNw7ORbJCBGYSbooam8tgsTkotiGShVRhCI4oQJqHkMtjMoj+BB0aSAsgeRoKtibbYeZZCeoAbiIxksURE1Golpd5isnh5zRCkTyRpERrSqSEdCgRBRUm0IDhKFCGFAQhQYRMmDAmxBIAAvgWgA
## - https://cloudspecs.fyi?state=N4IgzgjgNgigrgUwE4E8QC5wKggxgFwAIAzAQwFsBLKFAGkIAclLcEB9ACwHs4l6x8XJKQDm7XDwB2+foOFi2Ad2b52lLgzD1JCfGxEAjSvk20AOpMJXrVpFIAmACgFDR7ZcbUawhAFSEADj9CABYABgBOADZCAHpGLkVkRwBGMIAmEPoAZgBKenTcwlIfF3l2Q2NTCzN8WpwS-EcdPUqTLUIyt30jdqKSwg9VNgNFc0sbazs4SSch9lHYphZ2bl4C-p9RtgZkNnsuKChSPgtJ61wShEGOBEsmLlYwMCE2ci57bEIoSgBra4A5ABSACCAFkACJAgGEfC3SwA8EQmGKeHFJC4DiEAC8hABJ3IURCMLhdzxAHFhAA3YxcSQw7BgQEASWk2AZs2KPipdwOSAsADEAEoAeTBxUUYDYZCoNAsAHUABIAUSFys6cm6EhmRAAfIQwsVOV0FJQpZIqeRrqRjZrTVLnvYLEJPkhCAYUO7FDs9gcjidCJ8wLgLCBaCAkABhD4IDAgQRsMBUkSEAA8AFpOsmBMxJCJHIpKPY4TjCDwTHB8AA6QvFjhVyiSTH0W6UEQcIi48sMStV1vt6uN5udS4-POllK5GqSADEc8IuF4SDuRAgiFQhGXYDgUCIZsIjbx9mIAOdlZ7RAzhBEIgYUC4TWP9FICDAjhQ2O2uyQ+0Oxz4hAAB7YvMIxjAuhxCNiPKzEI9DHAY2DYgwjQII4MrUHQeJmBYAL0KBoz0AC5K9LEYAArklGEAA1Gc1hiFw5BsKogFNEUtETFYDFMQwXCNk0Ej3kg2IAiIy4oBRNE1CxKGzGwPxUPgb4fmE9DAWE7F0VYpJWmBjhTpIFh3g+jjdpWuQANyBggVJVlwxDEPpVkJkm+ZFDOZbnpWsJcLCtyEB25BQIwbhhiAxwoOWGCgPgKC7HGYB3sYIAAL4pUAA

variable "aws_instances" {
  type = map(string)
  description = "Instance type for the cluster members"
  default = {
    // "x86" = "c6in.8xlarge"
    // "x86" = "c5n.metal"
    //"x86" = "c6i.xlarge"
    //"arm" = "c6g.xlarge"
    // "x86" = "c6id.2xlarge" // 0
    // "arm" = "c7gd.2xlarge"
    // "x86" = "c6id.metal"
    // "x86" = "m6id.xlarge" // 0

    // "x86" = "c6id.32xlarge"
    "x86" = "c6id.metal"
    "arm" = "c7gd.metal"
    //"x86" = "c6in.xlarge"
    //"arm" = "c7gn.xlarge"
  }
}

// c7gn, c8gn not available in eu-central-1 apparently
// https://cloudspecs.fyi/?state=N4IgzgjgNgigrgUwE4E8QC5wKggxgFwAIAzAQwFsBLKFAGkIAclLcEB9ACwHs4l6x8XJKQDm7XDwB2+foOFi2Ad2b52lLgzD1JCfGxEAjSvk20AOpMJXrhJFIAmACh17DxzQHomLdt160AJkIASkJSMEI3PQZkNjgwe3NLGytccIRCRQ4ESyYuVjAwITZyLntsQihKAGsMgHIAUgBBAFkAEQa6wnxsyzrWtq6snLCkXA5CAF5COtIkcgA2ABYunpG6gHFhADdjLkku7DB6gElpbEPJezCI7Zz7IQtiO3IwxTA2MioaCyFypEIBhQkSM0Vi8Wu5TAuAsIFoICQAGEyggMCBBGwwNsRIQADwAWkIWJEAmYkhEjkUlHsPSmhB4Jjg+AAdFSaRxmZRJON6NlKCIOERpgyGEzmXyBSyuTyiWkquS6QBGYIWCwAYg1hFwvCQOSIEEQqFsCDAcCgREoES5M3sxDqvyZoqIBMiIgYUC4+EctvopBNjhQkyibBiSDiCXoAA9Ji59KDTFquB6kJM7lchPQoKQDNhJl9qChgqEANQWGxiLjkNiqSNekuqmsMUhXNhVKj4MAByYABijPfrySsa3I7AMikcKskFndnscIqZwQA3IRyttmVxiMQJ8uMcSJ4Q1fTHUzulxutlCILyFBGKJUfCsygGRhQPgUDE0WB3cYQABfX9AA
variable "aws_primary_instances" {
  type = map(string)
  description = "Instance type for the client node (needs higher network bandwidth to drive the benchmark all alone)"
  default = {
    // "x86" = "c5.metal" // magic-trace
    // "x86" = "c6in.4xlarge"

    // "x86" = "c6in.32xlarge"
    // "x86" = "c6id.metal"
    "x86" = "c6in.metal"
    "arm" = "c6gn.metal"
    // "arm" = "c6gd.xlarge"
  }
}

## ami ID depends on architecture and region
## https://cloud-images.ubuntu.com/locator/ec2/
variable "aws_amis" {
  type = map(map(string))
  description = "AMI IDs for regions and architectures"
  default = {
    "us-east-1" = { # 25.04
      "x86" = "ami-01f939902904966ba"
      "arm" = "ami-06d0e4df338014feb"
    }
    "us-east-2" = { # 25.04
      "x86" = "ami-022d4384c14dce184"
      "arm" = "ami-0e659ffa9eb8cf95a"
    }
    "eu-central-1" = { # 25.04
      "x86" = "ami-04baa57774212d79f"
      "arm" = "ami-0a9fc11d005810cf8"
    }
    "ap-southeast-1" = { # 25.04
      "x86" = "ami-059d2b65b19975037"
      "arm" = "ami-0ee6cea4a536d16e5"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "${var.aws_region}"
  shared_credentials_files = ["${var.aws_credentials_filepath}"]
  shared_config_files = ["${var.aws_config_filepath}"]
  profile = "default"
}

# ------------------------------------------------------------------------------
# IAM Role and Policy for S3 Access
# ------------------------------------------------------------------------------

# IAM policy document for EC2 to assume the role
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# IAM policy document for S3 access
data "aws_iam_policy_document" "s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:CreateBucket",
      "s3:DeleteBucket",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:GetBucketVersioning",
      "s3:PutBucketVersioning"
    ]
    resources = ["*"]
  }
  
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
      "s3:DeleteObjectVersion"
    ]
    resources = ["*"]
  }
}

# IAM role for EC2 instances
resource "aws_iam_role" "journal_node_role" {
  name               = "journal-node-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

# IAM policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "journal-node-s3-access"
  description = "Policy for journal nodes to access S3"
  policy      = data.aws_iam_policy_document.s3_access.json
}

# Attach the S3 policy to the role
resource "aws_iam_role_policy_attachment" "s3_policy_attachment" {
  role       = aws_iam_role.journal_node_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

# Instance profile for EC2 instances
resource "aws_iam_instance_profile" "journal_node_profile" {
  name = "journal-node-profile"
  role = aws_iam_role.journal_node_role.name
}
