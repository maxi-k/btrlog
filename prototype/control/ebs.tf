# ------------------------------------------------------------------------------
# EBS Volume Configuration
# ------------------------------------------------------------------------------

variable "enable_ebs" {
  description = "Enable EBS volume for experiments"
  type        = bool
  default     = false
}

variable "ebs_filesystem" {
  description = "Whether to automatically mount the EBS volume with a file system"
  type        = bool
  default     = false
}

variable "ebs_type" {
  description = "EBS volume type (io2, gp3)"
  type        = string
  default     = "gp3"
  validation {
    condition     = contains(["io2", "gp3"], var.ebs_type)
    error_message = "EBS volume type must be either 'io2' or 'gp3'."
  }
}

variable "ebs_size" {
  description = "EBS volume size in GB"
  type        = number
  default     = 300
}

variable "ebs_iops" {
  description = "Provisioned IOPS for the EBS volume"
  type        = number
  #default     = 256000
  # default     = 100000 # max io2
  default     = 80000 # max gp3
}

variable "ebs_device_name" {
  description = "Device name for the EBS volume"
  type        = string
  default     = "/dev/sdf"
}

variable "ebs_mount_point" {
  description = "Mount point for the EBS volume"
  type        = string
  default     = "/mnt/ebs-data"
}

# ------------------------------------------------------------------------------
# EBS Volume Resources
# ------------------------------------------------------------------------------

resource "aws_ebs_volume" "experiment_volume" {
  count = var.enable_ebs ? var.node_count : 0
  
  availability_zone = "${var.aws_region}${element(var.aws_az_suffixes, count.index % length(var.aws_az_suffixes))}"
  type              = var.ebs_type
  size              = var.ebs_size
  iops              = var.ebs_type == "io2" || var.ebs_type == "gp3" ? var.ebs_iops : null
  
  tags = {
    Name = "${var.node_name_prefix}${count.index}-ebs"
    NodeId = count.index
  }
}

resource "aws_volume_attachment" "experiment_volume_attachment" {
  count = var.enable_ebs ? var.node_count : 0
  
  device_name = var.ebs_device_name
  volume_id   = aws_ebs_volume.experiment_volume[count.index].id
  instance_id = aws_instance.journal_node[count.index].id
}

# Format and mount the EBS volume
resource "null_resource" "format_and_mount_ebs" {
  count = (var.enable_ebs && var.ebs_filesystem) ? var.node_count : 0
  
  depends_on = [aws_volume_attachment.experiment_volume_attachment]
  
  triggers = {
    volume_id = aws_ebs_volume.experiment_volume[count.index].id
  }
  
  connection {
    type        = "ssh"
    host        = aws_instance.journal_node[count.index].public_ip
    user        = var.node_default_user
    private_key = file(var.aws_sshkey_filepath)
  }
  
  provisioner "remote-exec" {
    inline = [
      # Wait for the device to be available
      "while [ ! -e ${var.ebs_device_name} ]; do sleep 5; done",
      
      # Check if the device is already formatted
      "if ! sudo blkid ${var.ebs_device_name}; then",
      "  echo 'Formatting EBS volume...'",
      "  sudo mkfs.ext4 ${var.ebs_device_name}",
      "else",
      "  echo 'EBS volume already formatted'",
      "fi",
      
      # Create mount point if it doesn't exist
      "sudo mkdir -p ${var.ebs_mount_point}",
      
      # Mount the volume
      "sudo mount ${var.ebs_device_name} ${var.ebs_mount_point}",
      
      # Add to fstab for persistent mounting
      "echo '${var.ebs_device_name} ${var.ebs_mount_point} ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab",
      
      # Set permissions
      "sudo chown ${var.node_default_user}:${var.node_default_user} ${var.ebs_mount_point}",
      "sudo chmod 755 ${var.ebs_mount_point}"
    ]
  }
}

# Output EBS volume information
output "ebs_volume_ids" {
  value = var.enable_ebs ? aws_ebs_volume.experiment_volume[*].id : []
}

output "ebs_mount_points" {
  value = var.enable_ebs ? [for i in range(var.node_count) : "${aws_instance.journal_node[i].public_ip}:${var.ebs_mount_point}"] : []
}
