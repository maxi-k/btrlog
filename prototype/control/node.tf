# ------------------------------------------------------------------------------
# Cluster setup - variables
# ------------------------------------------------------------------------------

variable "node_count" {
  description = "Number of members in the cluster"
  default = "4"
}

variable "partition_count" {
  description = "Number of members in the cluster"
  default = "1"
}

variable "node_name_prefix" {
  description = "Prefix to use when naming cluster members"
  default = "journal-node-"
}

variable "node_default_user" {
  description = "Default user for the AMI"
  default = "ubuntu"
}

variable "node_arch" {
  description = "Architecture of the node instances"
  # default = "arm"
  default = "x86"
}

variable "arch_interpreter_cmd" {
  description = "Path to the binary to deploy"
  default = {
    "x86" = "/lib64/$(ls /lib64)"
    "arm" = "/lib/ld-linux-aarch64.so.1"
  }
}

# BookKeeper deployment variables
variable "engine_binary" {
  description = "Name of the binary to deploy"
  # default = "bookkeeper-benchmark-1.0-SNAPSHOT.jar"
  # default = "LeanStore_YCSB"
  # default = "closed_bench"
  #default = "open_bench"
  default = "v2_bench"
  # default = ""
  # default = "open_ebs_bench"
  # default = "roofline_uring"
  # default = "test_blockdev"
}

variable "local_binary_directory" {
  description = "Path to the binary to deploy"
  default = {
    # "x86" = "../../cmp/leanstore/build/benchmark"
    # "arm" = "../../cmp/leanstore/build/benchmark"
    # "x86" = "../../cmp/bookkeeper/target"
    # "arm" = "../../cmp/bookkeeper/target"
    "x86" = "../target/release"
    "arm" = "../target/aarch64-unknown-linux-gnu/release"
    # "x86" = "../target/debug"
    # "arm" = "../target/aarch64-unknown-linux-gnu/debug"
  }
}

variable "placement_group_strategy" {
  description = "Placement group strategy: 'cluster' or 'partition' or 'spread'"
  default = "partition"
  validation {
    condition     = contains(["cluster", "partition", "spread"], var.placement_group_strategy)
    error_message = "Placement group strategy must be either 'cluster' or 'partition'."
  }
}

locals {
  remote_cluster_ips_file = "/tmp/cluster_ips"
  local_rootdir = "../"
  local_sourcedirs = "../../src"
  node_startup_script_file = "./scripts/node_startup.sh"
  # node_startup_script_file = "./scripts/node_startup_bookkeeper.sh"  # BookKeeper startup script
  node_keyname = "${trimspace(file(var.aws_sshkey_name))}"
  node_homedir = "/home/${var.node_default_user}"
}

# ------------------------------------------------------------------------------
# Archive the local binary file and its dependencies.
# ------------------------------------------------------------------------------
# Probably want to pull from S3 in production instead.
resource "null_resource" "create_engine_archive" {
  triggers = {
    always_run = "${timestamp()}"
  }

  # the archive_file data source doesn't have a way to glob files,
  # so we use a null_resource to create the archive.
  provisioner "local-exec" {
    # command = "cd ${var.local_binary_directory[var.node_arch]} && zip ${var.engine_binary}.zip ${var.engine_binary} ${local.local_sourcedirs}"
    command = "cd ${var.local_binary_directory[var.node_arch]} && zip ${var.engine_binary}.zip ${var.engine_binary}"  # For BookKeeper JAR
  }
}

data "local_file" "engine_archive" {
  depends_on = [null_resource.create_engine_archive]
  filename = "${var.local_binary_directory[var.node_arch]}/${var.engine_binary}.zip"
}

# ------------------------------------------------------------------------------
# Placement Group
# ------------------------------------------------------------------------------

# Create placement group for cluster instances
resource "aws_placement_group" "journal_cluster" {
  name         = "journal-pg-${var.placement_group_strategy}"
  strategy     = var.placement_group_strategy
  partition_count = var.placement_group_strategy == "partition" ? var.partition_count : null

  tags = {
    Name = "journal-cluster-placement-group"
  }
}

# ------------------------------------------------------------------------------
# Journal Node EC2 instance
# ------------------------------------------------------------------------------

# Create the journal node instances
resource "aws_instance" "journal_node" {
  ami                    = "${var.aws_amis[var.aws_region][var.node_arch]}"
  instance_type          = "${count.index == var.node_count - 1 ? var.aws_primary_instances[var.node_arch] : var.aws_instances[var.node_arch]}"
  availability_zone      = "${var.aws_region}${element(var.aws_az_suffixes, count.index % length(var.aws_az_suffixes))}"
  # networking
  subnet_id              = "${element(aws_subnet.journal_subnet_public.*.id, count.index % length(var.aws_az_suffixes))}"
  vpc_security_group_ids = ["${aws_security_group.allow_ssh.id}"]
  # access
  key_name               = "${local.node_keyname}"
  iam_instance_profile   = "${aws_iam_instance_profile.journal_node_profile.name}"
  ## startup script
  user_data = "${file(local.node_startup_script_file)}"
  # placement group for cluster experiments
  placement_group = aws_placement_group.journal_cluster.name
  placement_partition_number = var.placement_group_strategy == "partition" ? (count.index % var.partition_count) + 1 : null
  # cluster size
  count = "${var.node_count}"

  tags = {
    Name = "${var.node_name_prefix}${count.index}"
    ClusterSize = "${var.node_count}"
    NodeId = "${count.index}"
    LoggerInstance = "${var.aws_instances[var.node_arch]}"
    PrimaryInstance = "${var.aws_primary_instances[var.node_arch]}"
  }
  ## allow access to instance metadata on 169.254.169.254/latest/meta-data/tags/instance
  metadata_options  {
    instance_metadata_tags = "enabled"
    http_tokens = "optional"
  }

  ## terminate when instance shuts itself down
  ## not available if it's a spot instance
  instance_initiated_shutdown_behavior = "terminate"

  ## root volume configuration
  root_block_device {
    volume_size = 16  # GiB
  }

  ## comment if we want on-demand instances
  # instance_market_options {
  #   market_type = "spot"
  #   spot_options {
  #     max_price                      = "5.50"
  #     instance_interruption_behavior = "terminate"
  #     spot_instance_type             = "one-time"
  #   }
  # }
}

# Output the node ids, e.g., for replacement of individual nodes
# using terraform apply -replace=<id> or terraform taint <id> plus terraform apply
output "node_ids" {
  value = "${aws_instance.journal_node.*.id}"
}

# Output the public IP addresses of the journal node instances
output "node_public_ips" {
  # value = "${aws_instance.journal_node.*.public_ip}"
  value = "${aws_instance.journal_node.*.public_ip}"
}

# Output the private IP addresses of the journal_node instances
output "node_private_ips" {
  # value = "${aws_instance.journal_node.*.private_ip}"
  value = "${aws_instance.journal_node.*.private_ip}"
}

# Output SSH commands for instances for convenience
output "ssh_commands" {
  # value = "${join("\n", formatlist("ssh -i %s %s@%s", var.aws_sshkey_filepath, var.node_default_user, aws_instance.journal_node.*.public_ip))}"
  value = "${join("\n", formatlist("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s %s@%s", var.aws_sshkey_filepath, var.node_default_user, aws_instance.journal_node.*.public_ip))}"
}

# write the public IP addresses to a file for local scripts
resource "local_file" "saved_public_ips" {
  filename = "public_node_ips.txt"
  # value = "${aws_instance.journal_node.*.public_ip}"
  content = "${join("\n", aws_instance.journal_node.*.public_ip)}"
}

# write the ssh commands to a local file as well for convenience
resource "local_file" "saved_ssh_commands" {
  filename = "cluster_ssh_commands.txt"
  content = "${join("\n", formatlist("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s %s@%s", var.aws_sshkey_filepath, var.node_default_user, aws_instance.journal_node.*.public_ip))}"
}

# ------------------------------------------------------------------------------
# Populate /etc/hosts file on each instance of the cluster
# with the IP addresses of all other instances.
# ------------------------------------------------------------------------------

# Bash command to populate /etc/hosts file on each instances
resource "null_resource" "journal_node_ips" {
  count = "${var.node_count}"

  # Changes to any instance of the cluster requires re-provisioning
  triggers = {
    # cluster_instance_ids = "${join(",", aws_instance.journal_node.*.id)}"
    cluster_instance_ids = "${join(",", aws_instance.journal_node.*.id)}"
  }

  # Connect to the *public* IP using SSH
  connection {
    type = "ssh"
    # host = "${element(aws_instance.journal_node.*.public_ip, count.index)}"
    host = "${element(aws_instance.journal_node.*.public_ip, count.index)}"
    user = "${var.node_default_user}"
    private_key = "${file(var.aws_sshkey_filepath)}"
  }

  provisioner "remote-exec" {
    inline = [
      # Adds all cluster members' *private* IP addresses to /etc/hosts (on each member)
      # based on https://stackoverflow.com/questions/48184438/how-to-get-private-ip-of-ec-2-dynamically-and-put-it-in-etc-hosts
      # "echo '${join("\n", formatlist("%v", aws_instance.journal_node.*.private_ip))}' | awk '{ print $0 \" ${var.node_name_prefix}\" NR-1 }' | sudo tee -a /etc/hosts > ${local.remote_cluster_ips_file}",
      "echo '${join("\n", formatlist("%v", aws_instance.journal_node.*.private_ip))}' | awk '{ print $0 \" ${var.node_name_prefix}\" NR-1 }' | sudo tee -a /etc/hosts > ${local.remote_cluster_ips_file}",
      "sudo bash -c \"echo '${join("\n", formatlist("JOURNAL_NODE_IP_%v=%v", aws_instance.journal_node.*.tags.NodeId, aws_instance.journal_node.*.private_ip))}' >> /etc/environment\""
    ]
  }
}


# ------------------------------------------------------------------------------
# Unzip the binary file on the node instance
# ------------------------------------------------------------------------------


resource "null_resource" "deploy_engine" {
  count = "${var.node_count}"

  triggers = {
    archive_change = "${data.local_file.engine_archive.content_sha1}"
  }

  connection {
    type = "ssh"
    # host = "${element(aws_instance.journal_node.*.public_ip, count.index)}"
    host = "${element(aws_instance.journal_node.*.public_ip, count.index)}"
    user = "${var.node_default_user}"
    private_key = "${file(var.aws_sshkey_filepath)}"
  }

  # copy the binary + dependency zip to the node instance
  provisioner "file" {
    destination = "${local.node_homedir}/${var.engine_binary}.zip"
    source = "${data.local_file.engine_archive.filename}"
  }

  # unzip the binary file on the node instance
  provisioner "remote-exec" {
    inline = [
      # unzip the binary file on the node instance. Assumes unzip is installed or being installed by node startup script.
      "cd ${local.node_homedir} && ( while [ ! -f /usr/bin/unzip ]; do sleep 1; done ) && unzip -o ${var.engine_binary}.zip",
      # "rm -f ${local.node_homedir}/${var.engine_binary}.zip",
      "echo 'export LD_LIBRARY_PATH=\"${local.node_homedir}:$LD_LIBRARY_PATH\"' >> ${local.node_homedir}/.bashrc",
      # set interpreter in case it differs on build machine (e.g., nixos) - only for Rust binaries
      "cd ${local.node_homedir} && ( while [ ! -f /usr/bin/patchelf ]; do sleep 1; done ) && patchelf --set-interpreter ${var.arch_interpreter_cmd[var.node_arch]} ${var.engine_binary} || true",
      # For BookKeeper: make JAR accessible
      "chmod +r ${local.node_homedir}/${var.engine_binary}",
    ]
  }
}
