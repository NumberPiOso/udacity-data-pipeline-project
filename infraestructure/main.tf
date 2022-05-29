resource "aws_redshift_cluster" "redshift" {
  cluster_identifier     = "data-pipeline-project-pi"
  database_name          = "dev"
  master_username        = "awsuser"
  master_password        = "Awsuser123"
  node_type              = "dc2.large"
  cluster_type           = "single-node"
  publicly_accessible    = true
  enhanced_vpc_routing   = true
  skip_final_snapshot    = true
  vpc_security_group_ids = ["sg-02eb276a968cb2719"]
}
