variable "project" {
  
}

variable "GOOGLE_APPLICATION_CREDENTIALS" {
  
}
variable "region" {
    default = "us-central1"
}

variable "zone" {
  default = "us-central1-c"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset2"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "quiero_mi_bucket_seba_centurion"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
variable "location" {
  description = "Project Location"
  default     = "US"
}