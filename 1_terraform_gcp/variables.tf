variable "credentials" {
  description = "GCP Project Credentials"
  default = "./keys/my_creds.json"
}

variable "project" {
    description = "Project"
    default = "terraform-demo-433922"
}

variable "region" {
    description = "Project Region"
    default = "us-central1"
}

variable "location" {
    description = "Project Location"
    default = "US"
}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My GCS Bucket Name"
    default = "terraform-demo-433922-terraform-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}