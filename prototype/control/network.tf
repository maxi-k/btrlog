# ------------------------------------------------------------------------------
# Set up a VPC and subnets for the cluster
# ------------------------------------------------------------------------------

resource "aws_vpc" "journal_vpc" {
  cidr_block = "10.0.0.0/16"
  enable_dns_support = "true"
  enable_dns_hostnames = "true"

  tags = {
    Name = "journal-service-vpc"
  }
}

# public subnet
resource "aws_subnet" "journal_subnet_public" {
  count = "${length(var.aws_az_suffixes)}"

  vpc_id = "${aws_vpc.journal_vpc.id}"
  cidr_block = "${cidrsubnet("10.0.0.0/16", 8, count.index + 1)}"
  map_public_ip_on_launch = "true"
  availability_zone = "${var.aws_region}${element(var.aws_az_suffixes, count.index)}"
  tags = {
    Name = "journal-service-subnet-public-${element(var.aws_az_suffixes, count.index)}"
  }
}

# private subnet; unused for now
resource "aws_subnet" "journal_subnet_private" {
  count = "${length(var.aws_az_suffixes)}"

  vpc_id = "${aws_vpc.journal_vpc.id}"
  cidr_block = "${cidrsubnet("10.0.0.0/16", 8, count.index + 101)}"
  availability_zone = "${var.aws_region}${element(var.aws_az_suffixes, count.index)}"
  tags = {
    Name = "journal-service-subnet-private-${element(var.aws_az_suffixes, count.index)}"
  }
}

# ------------------------------------------------------------------------------
# Set up an internet gateway and routing table
# ------------------------------------------------------------------------------

# Add internet gateway
resource "aws_internet_gateway" "journal_gateway" {
  vpc_id = "${aws_vpc.journal_vpc.id}"
  tags = {
    Name = "journal-service-gateway"
  }
}

# Public routes
resource "aws_route_table" "journal_public_routes" {
  vpc_id = "${aws_vpc.journal_vpc.id}"

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.journal_gateway.id}"
  }

  tags = {
    Name = "journal-service-public-routes"
  }
}

resource "aws_route_table_association" "journal_public_subnet_association" {
  count = "${length(var.aws_az_suffixes)}"

  subnet_id = "${element(aws_subnet.journal_subnet_public.*.id, count.index)}"
  route_table_id = "${aws_route_table.journal_public_routes.id}"
}


# ------------------------------------------------------------------------------
# Allow SSH traffic to the cluster
# ------------------------------------------------------------------------------

resource "aws_security_group" "allow_ssh" {
  name        = "allow_ssh"
  description = "Allow SSH inbound traffic"
  vpc_id      = "${aws_vpc.journal_vpc.id}"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "journal-service-allows-ssh"
  }
}
