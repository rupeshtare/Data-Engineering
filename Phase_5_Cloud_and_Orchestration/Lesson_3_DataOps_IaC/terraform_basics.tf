# terraform_basics.tf
# Beginner Level: Defining Infrastructure as Code

# 1. PROVIDER (Who are we talking to?)
provider "aws" {
  region = "us-east-1"
}

# 2. THE RESOURCE (What are we building?)
# Let's create an S3 Bucket for our Data Lake
resource "aws_s3_bucket" "datalake" {
  bucket = "my-unique-data-engineering-bucket"
  
  tags = {
    Name        = "My Data Lake"
    Environment = "Dev"
  }
}

# 3. SECURITY (Who can use it?)
resource "aws_s3_bucket_public_access_block" "datalake_lock" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 🏛️ Architect's Tip:
# "Always block public access by default. It only takes one 
# misconfigured bucket to cause a major data leak. Use Terraform 
# to ensure security is built-in from day one."
