# In this file you can define the versions and configurations of the providers

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.14.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
  default_tags {
    tags = {
      project   = "data-pipeline-project"
      engineer  = "pi"
      comments  = "this resource is managed by terraform"
      terraform = "true"
    }
  }
}


