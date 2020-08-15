resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket
  acl    = var.acl

  tags = {
    Name        = var.name
    Environment = var.environment
  }
}

